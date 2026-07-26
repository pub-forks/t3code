#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off - node:os resolves the shared T3 home guard.
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/unstable/cli";

import { resolveWorktreeT3Home } from "@t3tools/shared/devHome";

import { DevSeedError, MAX_SEED_THREAD_LIMIT, seedDevDatabase } from "./lib/dev-seed.ts";
import { findRunningServerPid, findRunningServerPidSync } from "./lib/server-runtime-probe.ts";

const DEFAULT_THREAD_LIMIT = 25;
const DEFAULT_ACTIVITY_LIMIT = 200;

class DevSeedTargetError extends Schema.TaggedErrorClass<DevSeedTargetError>()(
  "DevSeedTargetError",
  {
    reason: Schema.Literals([
      "shared-home",
      "missing-target",
      "not-a-worktree",
      "server-running",
      "same-source",
    ]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "shared-home":
        return [
          `Refusing to seed ${this.detail}: that database lives in the shared T3 Code home.`,
          "This command overwrites projection tables. Run it from a worktree, or pass --to <base-dir>.",
        ].join("\n");
      case "missing-target":
        return [
          `No database at ${this.detail}.`,
          "Start the dev server once so migrations run (`bun run dev`), then seed.",
        ].join("\n");
      case "not-a-worktree":
        return [
          "Not inside a git worktree, so there is no worktree-local data directory to seed.",
          "Pass --to <base-dir> to choose one explicitly.",
        ].join("\n");
      case "server-running":
        return [
          `A T3 Code server (pid ${this.detail}) is running against this data directory.`,
          "Seeding replaces its projections and empties its event log while it holds them open,",
          "which leaves the running server's in-memory read model describing rows that no longer exist.",
          "Stop it, run the seed, then start it again.",
        ].join("\n");
      case "same-source":
        return [
          `Refusing to seed ${this.detail} from itself.`,
          "The copy would restore what the delete removed, but the event log and the",
          "projector cursors are cleared either way — so the directory loses its history",
          "and keeps only whatever the thread and activity caps selected.",
        ].join("\n");
    }
  }
}

/**
 * The state directory holding a base directory's database.
 *
 * `deriveServerPaths` picks `dev` for an implicit dev home and `userdata`
 * otherwise — a split that depends on how the server was started, which the
 * person seeding has no reason to know. Hard-coding `userdata` made a
 * main-checkout `bun run dev` (whose state lives in `<base>/dev`) unreachable:
 * the seed reported "no database, start the dev server so migrations run" about
 * a server that was already running.
 *
 * When both exist, neither is categorically right — a worktree run gets
 * `userdata` (the dev runner passes an explicit home), a main-checkout run gets
 * `dev` — so the tie goes to whichever was written most recently, i.e. the one
 * the last server to run actually opened. The chosen path is printed, so a
 * wrong guess is visible rather than silent.
 *
 * Recency is the newest of the database and its `-wal` sidecar. The server runs
 * SQLite in WAL mode, where writes land in the sidecar and the main file's
 * mtime can sit unchanged until a checkpoint — so reading the database alone
 * makes a busy directory look older than a dormant one.
 */
const resolveStateDir = Effect.fn("devSeed.resolveStateDir")(function* (baseDir: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const modifiedAtMs = (info: FileSystem.File.Info): number =>
    Option.match(info.mtime, { onSome: (mtime) => mtime.getTime(), onNone: () => 0 });

  const candidates: Array<{ readonly stateDir: string; readonly modifiedAtMs: number }> = [];
  for (const stateDir of ["userdata", "dev"] as const) {
    const dbPath = path.join(baseDir, stateDir, "state.sqlite");
    const info = yield* fileSystem.stat(dbPath).pipe(Effect.option);
    if (Option.isNone(info)) {
      continue;
    }
    // The sidecar is absent outside WAL mode, and after a clean checkpoint.
    const walInfo = yield* fileSystem.stat(`${dbPath}-wal`).pipe(Effect.option);
    candidates.push({
      stateDir,
      modifiedAtMs: Math.max(
        modifiedAtMs(info.value),
        Option.match(walInfo, { onSome: modifiedAtMs, onNone: () => 0 }),
      ),
    });
  }

  // `userdata` first above, so an equal or missing mtime keeps the old default.
  return (
    candidates.reduce<{ readonly stateDir: string; readonly modifiedAtMs: number } | undefined>(
      (best, candidate) =>
        best === undefined || candidate.modifiedAtMs > best.modifiedAtMs ? candidate : best,
      undefined,
    )?.stateDir ?? "userdata"
  );
});

const stateDbPath = (path: Path.Path, baseDir: string, stateDir: string) =>
  path.join(baseDir, stateDir, "state.sqlite");

/**
 * Copies the files behind the copied messages' attachment references.
 *
 * The rows carry only metadata; the bytes sit in a flat directory beside the
 * database as `<attachmentId><ext>` (apps/server/src/attachmentStore.ts). Copy
 * the rows alone and a seeded thread renders an image whose request 404s.
 *
 * Matched by directory listing rather than by reconstructing the extension: the
 * server infers it from mime type and file name at upload, and duplicating that
 * inference here would rot independently of it.
 */
const copyAttachments = Effect.fn("devSeed.copyAttachments")(function* (input: {
  readonly sourceDir: string;
  readonly targetDir: string;
  readonly attachmentIds: ReadonlyArray<string>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  if (input.attachmentIds.length === 0) {
    return 0;
  }

  const entries = yield* fileSystem
    .readDirectory(input.sourceDir)
    .pipe(Effect.orElseSucceed(() => [] as Array<string>));
  if (entries.length === 0) {
    return 0;
  }

  const wanted = new Set(input.attachmentIds);
  const matches = entries.filter((entry) => wanted.has(path.parse(entry).name));
  if (matches.length === 0) {
    return 0;
  }

  // Attachments are decoration on a dev fixture: one unreadable file should
  // leave the rest of the seed intact rather than fail a completed database
  // copy. The directory itself is subject to the same rule.
  return yield* fileSystem.makeDirectory(input.targetDir, { recursive: true }).pipe(
    Effect.andThen(
      Effect.forEach(matches, (entry) =>
        fileSystem
          .copyFile(path.join(input.sourceDir, entry), path.join(input.targetDir, entry))
          .pipe(
            Effect.as(1),
            Effect.orElseSucceed(() => 0),
          ),
      ),
    ),
    Effect.map((copied) => copied.reduce((total, one) => total + one, 0)),
    Effect.orElseSucceed(() => 0),
  );
});

const devSeedCli = Command.make("dev-seed", {
  from: Flag.string("from").pipe(
    Flag.withDescription(
      "Base directory to copy from (default: the shared T3 Code home, ~/.t3). Read-only.",
    ),
    Flag.optional,
    Flag.map(Option.getOrUndefined),
  ),
  to: Flag.string("to").pipe(
    Flag.withDescription(
      "Base directory to seed (default: this worktree's .t3). Its projection tables are replaced.",
    ),
    Flag.optional,
    Flag.map(Option.getOrUndefined),
  ),
  // Bounded below: SQLite reads a negative LIMIT as "no limit", which would
  // quietly turn a capped copy into a full-table one. The upper bound comes
  // from the seeder itself, which binds one variable per selected thread —
  // stating it separately here is what previously let the two drift apart.
  threads: Flag.integer("threads").pipe(
    Flag.withSchema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(MAX_SEED_THREAD_LIMIT)),
    ),
    Flag.withDescription(
      `How many recent threads to copy (default ${String(DEFAULT_THREAD_LIMIT)}, max ${String(MAX_SEED_THREAD_LIMIT)}).`,
    ),
    Flag.withDefault(DEFAULT_THREAD_LIMIT),
  ),
  activities: Flag.integer("activities").pipe(
    Flag.withSchema(Schema.Int.check(Schema.isGreaterThan(0))),
    Flag.withDescription(
      `Newest activities kept per thread (default ${String(DEFAULT_ACTIVITY_LIMIT)}).`,
    ),
    Flag.withDefault(DEFAULT_ACTIVITY_LIMIT),
  ),
}).pipe(
  Command.withDescription(
    "Copy recent projects and threads from a T3 Code data directory into an isolated dev one.",
  ),
  Command.withHandler((input) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const sharedHome = path.join(NodeOS.homedir(), ".t3");
      const sourceBaseDir = input.from ? path.resolve(input.from) : sharedHome;

      const defaultTarget = yield* resolveWorktreeT3Home(process.cwd());
      const targetBaseDir = input.to ? path.resolve(input.to) : defaultTarget;
      if (targetBaseDir === undefined) {
        return yield* new DevSeedTargetError({ reason: "not-a-worktree", detail: process.cwd() });
      }

      const [sourceStateDir, targetStateDir] = yield* Effect.all([
        resolveStateDir(sourceBaseDir),
        resolveStateDir(targetBaseDir),
      ]);
      const sourceDbPath = stateDbPath(path, sourceBaseDir, sourceStateDir);
      const targetDbPath = stateDbPath(path, targetBaseDir, targetStateDir);

      // The whole point is to keep dev data off the real home; overwriting it
      // here would be the exact accident this command exists to avoid.
      //
      // Guarded on the canonical *database* paths, not the base directories: a
      // base can canonicalize to itself while its `userdata` or `dev`
      // subdirectory is a symlink into the shared home, and comparing bases
      // then waves through a seed whose actual write target is the real
      // database. Resolve the full path first — the file itself can be the
      // symlink — and fall back to resolving the directory and re-joining the
      // filename when the file does not exist yet: a nonexistent file cannot
      // be a link to anything, but its directory still can be, and this is
      // exactly the case where the missing-target advice ("start the dev
      // server") would otherwise create the database in the shared home.
      const canonicalDbPath = (databasePath: string) =>
        fileSystem.realPath(databasePath).pipe(
          Effect.catch(() =>
            fileSystem
              .realPath(path.dirname(databasePath))
              .pipe(Effect.map((directory) => path.join(directory, path.basename(databasePath)))),
          ),
          Effect.orElseSucceed(() => databasePath),
        );
      const [canonicalTargetDb, canonicalSourceDb, canonicalShared] = yield* Effect.all([
        canonicalDbPath(targetDbPath),
        canonicalDbPath(sourceDbPath),
        fileSystem.realPath(sharedHome).pipe(Effect.orElseSucceed(() => sharedHome)),
      ]);
      if (canonicalTargetDb.startsWith(`${canonicalShared}${path.sep}`)) {
        return yield* new DevSeedTargetError({ reason: "shared-home", detail: canonicalTargetDb });
      }
      // After the shared-home refusal: that one must come first because the
      // missing-target advice below — start a dev server, which creates the
      // database — is exactly wrong for the shared home. Everywhere else, a
      // missing database is the more actionable diagnosis, so it outranks
      // same-source: aliased paths with nothing behind them are a setup
      // problem, not a self-seed.
      if (!(yield* fileSystem.exists(targetDbPath))) {
        return yield* new DevSeedTargetError({ reason: "missing-target", detail: targetDbPath });
      }

      // Seeding a database from itself is not the data loss it looks like —
      // the source reads a pre-transaction snapshot, so the copy puts the rows
      // back. What does not come back is the event log and the cursors, and
      // anything outside the caps. Nobody means to ask for that.
      if (canonicalTargetDb === canonicalSourceDb) {
        return yield* new DevSeedTargetError({ reason: "same-source", detail: canonicalTargetDb });
      }

      // SQLite locking is not enough on its own: the seed's transaction can take
      // the write lock between a running server's own writes and commit cleanly.
      // What it cannot do is tell that server the projections it is holding in
      // memory, and the event log it is appending to, were both replaced under
      // it. Its next write then lands on top of a read model it never built.
      //
      // Scoped to the state directory being seeded: a server using only the
      // other directory's database is not touching this one, and blocking on it
      // would refuse seeds that are perfectly safe.
      const runningPid = yield* findRunningServerPid(targetBaseDir, targetStateDir);
      if (Option.isSome(runningPid)) {
        return yield* new DevSeedTargetError({
          reason: "server-running",
          detail: String(runningPid.value),
        });
      }

      const seededAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
      const summary = yield* Effect.try({
        try: () =>
          seedDevDatabase({
            sourceDbPath,
            targetDbPath,
            threadLimit: input.threads,
            activityLimit: input.activities,
            seededAt,
            // The check above happens before the copy, which can take seconds.
            // Re-checked just before the commit so a server that started in
            // between aborts the seed instead of having its database swapped.
            findRunningServerPid: () => findRunningServerPidSync(targetBaseDir, targetStateDir),
          }),
        catch: (cause) => cause as DevSeedError,
      });

      // Attachments sit beside the database, so they follow the same state dir.
      const copiedAttachments = yield* copyAttachments({
        sourceDir: path.join(sourceBaseDir, sourceStateDir, "attachments"),
        targetDir: path.join(targetBaseDir, targetStateDir, "attachments"),
        attachmentIds: summary.attachmentIds,
      });

      yield* Console.log(
        [
          `Seeded ${targetDbPath}`,
          `  from     ${sourceDbPath}`,
          `  projects ${String(summary.projects)}`,
          `  threads  ${String(summary.threads)}`,
          `  messages ${String(summary.messages)}`,
          `  activity ${String(summary.activities)}`,
          `  turns    ${String(summary.turns)}  sessions ${String(summary.sessions)}`,
          ...(summary.attachmentIds.length > 0
            ? [
                `  files    ${String(copiedAttachments)}/${String(summary.attachmentIds.length)} attachment(s)`,
              ]
            : []),
          ...(summary.skippedColumns.length > 0
            ? [
                `  note: skipped ${String(summary.skippedColumns.length)} column(s) absent from the target schema`,
                `        (${summary.skippedColumns.join(", ")})`,
              ]
            : []),
          "",
          "Restart the dev server to pick it up.",
        ].join("\n"),
      );
    }).pipe(
      Effect.tapError((error) =>
        Effect.logError(
          error instanceof DevSeedError
            ? `${error.message}${error.hint ? `\n${error.hint}` : ""}`
            : error.message,
        ),
      ),
    ),
  ),
);

if (import.meta.main) {
  Command.run(devSeedCli, { version: "0.0.0" }).pipe(
    Effect.provide(Layer.mergeAll(Logger.layer([Logger.consolePretty()]), NodeServices.layer)),
    NodeRuntime.runMain,
  );
}
