/**
 * Copies recent projects and threads from one T3 Code database into another, so
 * an isolated dev server opens on something recognisable instead of an empty
 * sidebar.
 *
 * Projections only — no event history is copied, and the target's own log is
 * emptied. The copied projections describe a different world than whatever the
 * target recorded, so replaying its retained events over them would resurrect
 * threads and projects it had deleted; copying a *partial* source range is
 * worse still, since the projector would replay a tail whose creating events
 * are missing. With the log empty and every projector cursor at 0, bootstrap
 * streams nothing and leaves the copied rows alone.
 *
 * Everything that would otherwise wait on an agent is settled too — sessions,
 * turns, streaming messages, badge counts, provider bindings — because no agent
 * process comes with the copy. See
 * .agents/skills/test-t3-app/references/sqlite-fixtures.md.
 */

import * as NodeSqlite from "node:sqlite";

import { PROJECTION_TABLES_IN_DEPENDENCY_ORDER, PROJECTOR_NAMES } from "./projection-tables.ts";

/**
 * The most threads one call can copy: SQLite's default
 * SQLITE_MAX_VARIABLE_NUMBER, because the statements below bind one variable
 * per selected thread.
 *
 * Exported so the CLI's `--threads` bound comes from the code that actually has
 * to satisfy it. When the two were stated separately, a statement that bound
 * two extra literals put the CLI's own documented maximum over the ceiling —
 * and it failed after the target had been emptied. Anything added to those
 * statements has to stay free of per-call bindings, or this has to come down.
 */
export const MAX_SEED_THREAD_LIMIT = 32_766;

/** Inputs to {@link seedDevDatabase}. Both limits must be positive. */
export interface DevSeedOptions {
  readonly sourceDbPath: string;
  /** Emptied and rewritten. Must not be a database a server has open. */
  readonly targetDbPath: string;
  /** How many recent threads to copy. Capped at {@link MAX_SEED_THREAD_LIMIT}. */
  readonly threadLimit: number;
  /**
   * Newest activities kept per thread. The real table runs to six figures, and
   * the tail is what makes a thread look alive, so a cap keeps the copy quick
   * without making it look empty.
   */
  readonly activityLimit: number;
  /** ISO-8601 timestamp stamped on the projector cursor rows. */
  readonly seededAt: string;
  /**
   * Re-checked just before the commit, after every destructive statement has
   * run but while a rollback still costs nothing. The caller's own check
   * happens before this function is entered, leaving a window in which a server
   * can start; returning a pid here aborts rather than persisting a swap
   * underneath it.
   *
   * Cannot close the window completely — a server starting between this call
   * and the commit is still possible, since a filesystem read and a SQLite
   * transaction have no common lock — but it shrinks it from the whole copy to
   * the commit itself.
   */
  readonly findRunningServerPid?: () => number | undefined;
}

/** What {@link seedDevDatabase} copied, for the CLI's summary output. */
export interface DevSeedSummary {
  readonly projects: number;
  readonly threads: number;
  readonly messages: number;
  readonly activities: number;
  readonly turns: number;
  readonly sessions: number;
  readonly skippedColumns: ReadonlyArray<string>;
  /**
   * Attachment ids the copied messages reference. The rows carry the metadata
   * but the bytes live on disk beside the database, so the caller has to copy
   * those files too — otherwise a seeded thread renders an image that 404s.
   */
  readonly attachmentIds: ReadonlyArray<string>;
}

/**
 * A seed that could not proceed. `hint` carries the actionable half — the
 * underlying cause, or what to do about it — which the CLI prints on its own
 * line beneath the message.
 */
export class DevSeedError extends Error {
  override readonly name = "DevSeedError";
  readonly hint: string | undefined;
  constructor(message: string, hint?: string) {
    super(message);
    this.hint = hint;
  }
}

const columnsOf = (database: NodeSqlite.DatabaseSync, table: string): ReadonlyArray<string> =>
  database
    .prepare(`SELECT name FROM pragma_table_info(?)`)
    .all(table)
    .map((row) => String((row as { name: unknown }).name));

/**
 * Columns present in both databases. The two can sit on different migrations —
 * a dev worktree is often a migration behind or ahead of the installed app — so
 * `SELECT *` would fail on the first schema change. Copying the intersection
 * degrades gracefully instead: a column only the target knows about keeps its
 * default.
 */
function sharedColumns(
  source: NodeSqlite.DatabaseSync,
  target: NodeSqlite.DatabaseSync,
  table: string,
): { readonly shared: ReadonlyArray<string>; readonly skipped: ReadonlyArray<string> } {
  const sourceColumns = columnsOf(source, table);
  const targetColumns = new Set(columnsOf(target, table));
  const shared = sourceColumns.filter((column) => targetColumns.has(column));
  const skipped = sourceColumns
    .filter((column) => !targetColumns.has(column))
    .map((column) => `${table}.${column}`);
  return { shared, skipped };
}

const placeholders = (count: number) => Array.from({ length: count }, () => "?").join(", ");

const hasTable = (database: NodeSqlite.DatabaseSync, table: string): boolean =>
  database.prepare(`SELECT 1 FROM pragma_table_info(?)`).get(table) !== undefined;

const hasColumn = (database: NodeSqlite.DatabaseSync, table: string, column: string): boolean =>
  columnsOf(database, table).includes(column);

/**
 * Attachment ids referenced by the copied messages, so the caller can copy the
 * files that back them. `attachments_json` is a JSON array of ChatAttachment
 * (packages/contracts/src/orchestration.ts); anything unparseable is skipped
 * rather than failing the seed, since a missing image is a cosmetic problem and
 * an aborted seed is not.
 */
function collectAttachmentIds(
  target: NodeSqlite.DatabaseSync,
  threadIds: ReadonlyArray<string>,
): ReadonlyArray<string> {
  if (
    threadIds.length === 0 ||
    !hasColumn(target, "projection_thread_messages", "attachments_json")
  ) {
    return [];
  }
  const ids = new Set<string>();
  const rows = target
    .prepare(
      `SELECT attachments_json FROM projection_thread_messages
       WHERE attachments_json IS NOT NULL AND thread_id IN (${placeholders(threadIds.length)})`,
    )
    .iterate(...threadIds) as Iterable<{ attachments_json: unknown }>;
  for (const row of rows) {
    if (typeof row.attachments_json !== "string") {
      continue;
    }
    try {
      const parsed: unknown = JSON.parse(row.attachments_json);
      if (!Array.isArray(parsed)) {
        continue;
      }
      for (const attachment of parsed) {
        const id: unknown = (attachment as { id?: unknown } | null)?.id;
        if (typeof id === "string" && id.length > 0) {
          ids.add(id);
        }
      }
    } catch {
      // A row whose JSON does not parse cannot name a file to copy.
    }
  }
  return [...ids];
}

/**
 * Copies rows for `table` whose `keyColumn` is in `keys`, optionally keeping
 * only the newest `perKeyLimit` rows per key.
 */
function copyRows(input: {
  readonly source: NodeSqlite.DatabaseSync;
  readonly target: NodeSqlite.DatabaseSync;
  readonly table: string;
  readonly keyColumn: string;
  readonly keys: ReadonlyArray<string>;
  readonly omitColumns?: ReadonlyArray<string>;
  readonly perKeyLimit?: { readonly orderBy: string; readonly limit: number };
  readonly overrides?: Readonly<Record<string, unknown>>;
}): { readonly copied: number; readonly skipped: ReadonlyArray<string> } {
  if (input.keys.length === 0) {
    return { copied: 0, skipped: [] };
  }

  const { shared, skipped } = sharedColumns(input.source, input.target, input.table);
  const omit = new Set(input.omitColumns ?? []);
  const columns = shared.filter((column) => !omit.has(column));
  if (columns.length === 0) {
    return { copied: 0, skipped };
  }

  const selectList = columns.map((column) => `"${column}"`).join(", ");
  // OR REPLACE never fires after the wholesale DELETE, but keeps the copy
  // robust if the delete list and the copy list ever drift apart.
  const insert = input.target.prepare(
    `INSERT OR REPLACE INTO ${input.table} (${selectList}) VALUES (${placeholders(columns.length)})`,
  );
  const overrides = input.overrides ?? {};
  let copied = 0;
  // Iterate rather than materialize: messages are uncapped, and buffering
  // every row of a large copy into a JS array costs memory for nothing —
  // the target transaction is already open.
  const insertFrom = (rows: Iterable<unknown>) => {
    for (const row of rows as Iterable<Record<string, unknown>>) {
      insert.run(
        ...columns.map((column) => {
          const value = Object.hasOwn(overrides, column) ? overrides[column] : row[column];
          // node:sqlite binds only null/number/bigint/string/Uint8Array; every
          // projection column is one of those, and undefined means "absent".
          return (value ?? null) as null | number | bigint | string | Uint8Array;
        }),
      );
      copied += 1;
    }
  };

  if (input.perKeyLimit) {
    // Per-key cap: one bounded query per key beats a window function — the
    // (thread_id, created_at) index lets each query walk backwards and stop
    // at the limit instead of ranking every row.
    const statement = input.source.prepare(
      `SELECT ${selectList} FROM ${input.table} WHERE "${input.keyColumn}" = ?
       ORDER BY ${input.perKeyLimit.orderBy} DESC LIMIT ?`,
    );
    for (const key of input.keys) {
      insertFrom(statement.iterate(key, input.perKeyLimit.limit));
    }
  } else {
    insertFrom(
      input.source
        .prepare(
          `SELECT ${selectList} FROM ${input.table} WHERE "${input.keyColumn}" IN (${placeholders(input.keys.length)})`,
        )
        .iterate(...input.keys),
    );
  }

  return { copied, skipped };
}

/**
 * Settles turns copied while still running, matching what the server does to a
 * turn whose session leaves "running": `settledTurnStateForSessionStatus` in
 * apps/server/src/orchestration/Layers/ProjectionPipeline.ts maps the "stopped"
 * status the sessions are copied with to "interrupted".
 *
 * "pending" rows are deleted rather than settled — see `dropPendingTurnStarts`.
 *
 * An UPDATE rather than a copy override, because only the rows that were
 * actually running should change; the rest keep their real recorded state.
 */
function settleRunningTurns(
  target: NodeSqlite.DatabaseSync,
  threadIds: ReadonlyArray<string>,
): void {
  if (threadIds.length === 0 || !hasColumn(target, "projection_turns", "state")) {
    return;
  }
  // A settled turn needs a completion time, and `requested_at` is the only
  // timestamp guaranteed present. Overwritten unconditionally rather than
  // COALESCEd: a running turn can carry a stale `completed_at` from an earlier
  // settle that a later checkpoint reverted to "running" (the `...existingTurn`
  // spread in ProjectionPipeline preserves it), and keeping that value would
  // date the interruption to whenever that checkpoint landed.
  const setCompletedAt = hasColumn(target, "projection_turns", "completed_at")
    ? `, completed_at = requested_at`
    : "";
  // `turn_id IS NOT NULL` mirrors the server's own settle filter
  // (ProjectionPipeline, on the session leaving "running"). No writer produces
  // a row that is both null-id and running today — the only null-id insert
  // writes 'pending' — so this is guarding the invariant rather than a case,
  // and it keeps the two predicates comparable if that ever changes.
  //
  // 'running' is inlined, not bound: `--threads` is capped at SQLite's variable
  // ceiling, so every binding beyond one per thread puts the maximum over the
  // limit. Safe to interpolate — an internal literal, never input.
  target
    .prepare(
      `UPDATE projection_turns SET state = 'interrupted'${setCompletedAt}
       WHERE state = 'running'
         AND turn_id IS NOT NULL
         AND thread_id IN (${placeholders(threadIds.length)})`,
    )
    .run(...threadIds);
}

/**
 * Drops turn rows copied in the "pending" state — a user message accepted
 * before its provider turn started.
 *
 * Deleted rather than settled, because that is what the server does with them:
 * `clearPendingProjectionTurnsByThread` removes them once the turn starts or
 * the thread is stopped, and nothing settles them in place. They also cannot be
 * settled coherently — `turn_id` is NULL, so an "interrupted" pending row is a
 * terminal turn with no id, a shape no real turn ever has.
 *
 * Matches the server's own predicate exactly, so a checkpoint row is never
 * caught by it.
 */
function dropPendingTurnStarts(
  target: NodeSqlite.DatabaseSync,
  threadIds: ReadonlyArray<string>,
): void {
  if (threadIds.length === 0 || !hasColumn(target, "projection_turns", "state")) {
    return;
  }
  // The checkpoint guard is only meaningful where the column exists; an older
  // target without it has no checkpoint rows to protect.
  const excludeCheckpoints = hasColumn(target, "projection_turns", "checkpoint_turn_count")
    ? "AND checkpoint_turn_count IS NULL"
    : "";
  target
    .prepare(
      `DELETE FROM projection_turns
       WHERE state = 'pending'
         AND turn_id IS NULL
         ${excludeCheckpoints}
         AND thread_id IN (${placeholders(threadIds.length)})`,
    )
    .run(...threadIds);
}

/**
 * Replaces the target's projections with a recent slice of the source's.
 *
 * Destructive and not incremental: every projection table, the event log, and
 * the command receipts are emptied first, inside one transaction that rolls
 * back on any failure. Validation that can reject the whole run happens before
 * that transaction opens, so a rejected seed leaves the target as it was.
 *
 * The caller must ensure no server is using the target — see the guard in
 * scripts/dev-seed.ts. Returns counts plus the attachment ids whose files the
 * caller still has to copy.
 *
 * @throws {DevSeedError} on bad limits, an unopenable database, or a source
 * with no active threads.
 */
export function seedDevDatabase(options: DevSeedOptions): DevSeedSummary {
  // Checked before anything is opened, and certainly before the target is
  // emptied: overrunning the ceiling mid-copy destroys the target's data and
  // rolls back without replacing it.
  if (options.threadLimit > MAX_SEED_THREAD_LIMIT) {
    throw new DevSeedError(
      `cannot copy more than ${String(MAX_SEED_THREAD_LIMIT)} threads in one pass`,
      "Each thread costs one SQLite bound variable, and that is the engine's limit.",
    );
  }
  // Both limits are bound straight into `LIMIT ?`, where SQLite reads a
  // negative value as "no limit" — so a caller bypassing the CLI flags turns a
  // capped copy into a full-table one, silently. The flags already reject this;
  // enforcing it here keeps the options contract true of the function itself.
  for (const [name, value] of [
    ["threadLimit", options.threadLimit],
    ["activityLimit", options.activityLimit],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new DevSeedError(
        `${name} must be a positive integer, got ${String(value)}`,
        "SQLite reads a negative LIMIT as unbounded, which would copy the whole table.",
      );
    }
  }

  let source: NodeSqlite.DatabaseSync;
  try {
    source = new NodeSqlite.DatabaseSync(options.sourceDbPath, { readOnly: true });
  } catch (cause) {
    throw new DevSeedError(
      `could not open the source database at ${options.sourceDbPath}`,
      `${String(cause)}. Has T3 Code run at least once?`,
    );
  }

  let target: NodeSqlite.DatabaseSync;
  try {
    target = new NodeSqlite.DatabaseSync(options.targetDbPath);
  } catch (cause) {
    source.close();
    throw new DevSeedError(
      `could not open the target database at ${options.targetDbPath}`,
      `${String(cause)}. Start the dev server once so migrations run, then retry.`,
    );
  }

  // The source is normally the *live* installed app's database — only the
  // target is guarded against a running server, because reading is harmless.
  // Reading it across a dozen separate implicit transactions is not: the app
  // can delete a project between the query that picks it up and the copy that
  // reads its row, leaving a copied thread whose project no longer exists (no
  // foreign keys are declared, so nothing catches it). One deferred read
  // transaction pins every query below to a single snapshot.
  let sourceTransactionOpen = false;
  try {
    source.exec("BEGIN");
    sourceTransactionOpen = true;
  } catch {
    // A concurrent writer can leave the connection unable to start one. The
    // copy is still worth doing — it just loses snapshot isolation.
  }

  try {
    // Threads the user actually touched most recently. Mirrors the sidebar's
    // own ordering (packages/client-runtime/src/state/threadSort.ts), including
    // its user-message scan when the materialized timestamp is null. Both
    // recency columns arrived in later migrations, so an older source database
    // is read with whichever inputs it actually has.
    const recencyExpressions: Array<string> = [];
    if (hasColumn(source, "projection_threads", "latest_user_message_at")) {
      recencyExpressions.push('threads."latest_user_message_at"');
    }
    if (
      hasTable(source, "projection_thread_messages") &&
      ["thread_id", "role", "created_at"].every((column) =>
        hasColumn(source, "projection_thread_messages", column),
      )
    ) {
      recencyExpressions.push(`(
        SELECT MAX(messages."created_at")
        FROM projection_thread_messages AS messages
        WHERE messages."thread_id" = threads."thread_id"
          AND messages."role" = 'user'
      )`);
    }
    for (const column of ["updated_at", "created_at"]) {
      if (hasColumn(source, "projection_threads", column)) {
        recencyExpressions.push(`threads."${column}"`);
      }
    }
    // SQLite's COALESCE requires at least two arguments, so a source down to a
    // single recency column — the pre-migration-017 case this filter exists to
    // support — would throw rather than degrade. `created_at` is NOT NULL from
    // migration 005 on, so there is always at least one.
    const recencyOrder =
      recencyExpressions.length > 1
        ? `COALESCE(${recencyExpressions.join(", ")})`
        : (recencyExpressions[0] ?? "threads.rowid");
    const activeFilters = ["deleted_at", "archived_at"]
      .filter((column) => hasColumn(source, "projection_threads", column))
      .map((column) => `threads."${column}" IS NULL`);
    const threadIds = (
      source
        .prepare(
          `SELECT threads."thread_id" AS thread_id
           FROM projection_threads AS threads
           ${activeFilters.length > 0 ? `WHERE ${activeFilters.join(" AND ")}` : ""}
           ORDER BY ${recencyOrder} DESC, threads."thread_id" DESC
           LIMIT ?`,
        )
        .all(options.threadLimit) as Array<{ thread_id: string }>
    ).map((row) => row.thread_id);

    if (threadIds.length === 0) {
      throw new DevSeedError(
        "the source database has no active threads to copy",
        "Use T3 Code normally first, or point --from at a different data directory.",
      );
    }

    const projectIds = (
      source
        .prepare(
          `SELECT DISTINCT project_id FROM projection_threads
           WHERE thread_id IN (${placeholders(threadIds.length)})`,
        )
        .all(...threadIds) as Array<{ project_id: string }>
    ).map((row) => row.project_id);

    const skipped: Array<string> = [];
    const record = (result: {
      readonly copied: number;
      readonly skipped: ReadonlyArray<string>;
    }) => {
      skipped.push(...result.skipped);
      return result.copied;
    };

    target.exec("BEGIN IMMEDIATE");

    for (const table of PROJECTION_TABLES_IN_DEPENDENCY_ORDER) {
      // A target behind on migrations may not have every table yet; skipping is
      // consistent with how column drift is handled, and beats aborting the seed.
      if (hasTable(target, table)) {
        target.exec(`DELETE FROM ${table}`);
      }
    }

    // Projections are copied wholesale, so any event history the target still
    // holds describes a different world. Replaying it over the copied rows
    // would resurrect the target's own deleted threads and projects.
    if (hasTable(target, "orchestration_events")) {
      target.exec("DELETE FROM orchestration_events");
    }
    if (hasTable(target, "orchestration_command_receipts")) {
      target.exec("DELETE FROM orchestration_command_receipts");
    }
    // Provider bindings are keyed by thread id and are not copied (no agent
    // process comes with the seed). Left in place they outlive the threads they
    // name: ProviderSessionReaper sweeps every non-stopped binding, finds no
    // thread behind it, and so never hits its active-turn guard — it just tries
    // to stop a session for a thread that no longer exists, against a provider
    // instance this worktree may not even define.
    if (hasTable(target, "provider_session_runtime")) {
      target.exec("DELETE FROM provider_session_runtime");
    }

    const projects = record(
      copyRows({
        source,
        target,
        table: "projection_projects",
        keyColumn: "project_id",
        keys: projectIds,
      }),
    );
    const threads = record(
      copyRows({
        source,
        target,
        table: "projection_threads",
        keyColumn: "thread_id",
        keys: threadIds,
        // Approvals are not copied (see below), so the badge must not claim any.
        // Sessions are copied as stopped with no active turn, matching the
        // thread.session-set projection by clearing its latest turn pointer.
        overrides: {
          latest_turn_id: null,
          pending_approval_count: 0,
          pending_user_input_count: 0,
        },
      }),
    );
    // row_id is an AUTOINCREMENT surrogate; let the target assign its own.
    const turns = record(
      copyRows({
        source,
        target,
        table: "projection_turns",
        keyColumn: "thread_id",
        keys: threadIds,
        omitColumns: ["row_id"],
      }),
    );
    // A thread copied mid-turn has no agent to finish it. The session status is
    // forced to "stopped" below, but the turn is read independently: an
    // unfinished turn keeps the thread unsettled and unfoldable forever
    // (deriveUnsettledTurnId in MessagesTimeline.logic.ts). Interrupted is what
    // the server itself settles an abandoned turn to; a pending start is
    // something it deletes outright.
    settleRunningTurns(target, threadIds);
    dropPendingTurnStarts(target, threadIds);
    const messages = record(
      copyRows({
        source,
        target,
        table: "projection_thread_messages",
        keyColumn: "thread_id",
        keys: threadIds,
        // Same reason: a message copied while streaming has nothing left to
        // stream into it, so it would render with a caret that never resolves.
        overrides: { is_streaming: 0 },
      }),
    );
    const activities = record(
      copyRows({
        source,
        target,
        table: "projection_thread_activities",
        keyColumn: "thread_id",
        keys: threadIds,
        // Ordered the way the app reads these back — `sequence`, `created_at`,
        // then `activity_id` (ProjectionSnapshotQuery). Any other key order can keep
        // rows the timeline treats as oldest: timestamps tie within a burst of
        // tool activity, and can even disagree with `sequence` outright when
        // events arrive with skewed clocks. Descending, a NULL `sequence`
        // sorts last, mirroring where the app's ascending reads place it.
        //
        // `sequence` arrived in migration 008, so a source without it falls
        // back to the timestamp — same drift tolerance as the rest of the copy.
        perKeyLimit: {
          orderBy: hasColumn(source, "projection_thread_activities", "sequence")
            ? `"sequence" DESC, "created_at" DESC, "activity_id"`
            : `"created_at" DESC, "activity_id"`,
          limit: options.activityLimit,
        },
      }),
    );
    const sessions = record(
      copyRows({
        source,
        target,
        table: "projection_thread_sessions",
        keyColumn: "thread_id",
        keys: threadIds,
        // No agent process is attached in the copy. A carried-over "running"
        // status with an active turn renders a thread that spins forever, and
        // ProviderSessionReaper skips reaping anything with an active turn.
        // "ready"/"idle" are live-session completion signals to agent
        // awareness, so preserving those would publish historical fixtures as
        // newly completed work. Every copied session is intentionally stopped.
        overrides: { status: "stopped", active_turn_id: null, last_error: null },
      }),
    );
    record(
      copyRows({
        source,
        target,
        table: "projection_thread_proposed_plans",
        keyColumn: "thread_id",
        keys: threadIds,
      }),
    );
    // projection_pending_approvals is deliberately skipped: migration 025 deletes
    // approvals with no matching `approval.requested` activity, and the activity
    // cap above can easily drop it.

    // Required: computeSnapshotSequence returns 0 unless every projector has a
    // row, which makes every shell snapshot advertise sequence 0.
    //
    // Zero, not the source's cursors. `orchestration_events.sequence` is
    // AUTOINCREMENT, so emptying the log does not reset its high-water mark and
    // the next real event continues from wherever the target left off. A cursor
    // carried over from the source is unrelated to that number: it could sit
    // above it (each projector then silently skipping a different count of the
    // user's first real events, desynchronizing the projections for good) or
    // below it (replaying events over rows they never produced). Zero is below
    // every future sequence, so bootstrap streams the empty log, finds nothing,
    // and leaves the copied rows exactly as they are.
    const insertState = target.prepare(
      `INSERT OR REPLACE INTO projection_state (projector, last_applied_sequence, updated_at)
       VALUES (?, 0, ?)`,
    );
    for (const projector of PROJECTOR_NAMES) {
      insertState.run(projector, options.seededAt);
    }

    // Read back inside the transaction: these are the rows that were just
    // written, not the source's, so a column the target lacks is already gone.
    const attachmentIds = collectAttachmentIds(target, threadIds);

    // Last moment a rollback is still free. The caller checked before calling,
    // but a server can start during a copy that takes seconds; checking again
    // here narrows the window to the commit itself. Everything above is inside
    // the transaction, so aborting now leaves the target exactly as it was.
    const lateRunningPid = options.findRunningServerPid?.();
    if (lateRunningPid !== undefined) {
      throw new DevSeedError(
        `a T3 Code server (pid ${String(lateRunningPid)}) started while the seed was running`,
        "Nothing was changed. Stop it and run the seed again.",
      );
    }

    target.exec("COMMIT");

    return {
      projects,
      threads,
      messages,
      activities,
      turns,
      sessions,
      skippedColumns: [...new Set(skipped)].sort(),
      attachmentIds,
    };
  } catch (cause) {
    try {
      target.exec("ROLLBACK");
    } catch {
      // Already rolled back, or the transaction never opened.
    }
    throw cause instanceof DevSeedError
      ? cause
      : new DevSeedError(`could not seed the dev database: ${String(cause)}`);
  } finally {
    if (sourceTransactionOpen) {
      try {
        // Read-only, so there is nothing to commit; end it before closing so
        // the snapshot is released explicitly rather than by teardown.
        source.exec("ROLLBACK");
      } catch {
        // Already ended, which is equally fine for a read-only transaction.
      }
    }
    source.close();
    target.close();
  }
}
