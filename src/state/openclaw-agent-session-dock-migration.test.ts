import { constants, DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { OPENCLAW_AGENT_SCHEMA_VERSION } from "./openclaw-agent-db-contract.js";
import { withAgentDatabaseMaintenanceLease } from "./openclaw-agent-db-maintenance-lease.js";
import { getOpenClawAgentMigrationSchema } from "./openclaw-agent-db-schema-helpers.js";
import { ensureOpenClawAgentDatabaseSchema } from "./openclaw-agent-db-schema.js";

function seedV24(database: DatabaseSync) {
  database.exec(getOpenClawAgentMigrationSchema(24));
  database.exec(`
    PRAGMA user_version = 24;
    INSERT INTO schema_meta(meta_key, role, schema_version, agent_id, app_version, created_at, updated_at)
      VALUES ('primary', 'agent', 24, 'main', '2026.9.8', 1, 1);
    ALTER TABLE session_nodes ADD COLUMN future_metadata TEXT;
    INSERT INTO session_nodes
      (rowid, session_key, current_session_id, entry_json, updated_at, created_via,
       created_actor_type, created_actor_id, future_metadata)
      VALUES (41, 'agent:main:legacy', 'transcript',
        '{"sessionId":"transcript","updatedAt":1,"createdVia":"operator","createdActor":{"type":"human","source":"profile","id":"creator"},"sandbox":"required"}',
        1, 'operator', 'human', 'creator', 'preserved');
    INSERT INTO session_windows(session_id, session_key, created_at, updated_at)
      VALUES ('transcript', 'agent:main:legacy', 1, 1);
    INSERT INTO transcript_events(session_id, seq, event_json, created_at)
      VALUES ('transcript', 1, ' {"type":"custom","data":"original bytes"}\n', 1);
    INSERT INTO session_entry_snapshots(session_key, field, value_json)
      VALUES ('agent:main:legacy', 'skillsSnapshot', '{"prompt":"retained","skills":[]}');
    INSERT INTO session_members(session_key, identity_id, added_by, added_at)
      VALUES ('agent:main:legacy', 'member', 'creator', 1);
    CREATE VIEW fixture_session_names AS SELECT session_key FROM session_nodes;
    CREATE TRIGGER fixture_session_names_delete INSTEAD OF DELETE ON fixture_session_names
    BEGIN DELETE FROM session_nodes WHERE session_key = OLD.session_key; END;
  `);
}

function preservedRows(database: DatabaseSync) {
  return {
    nodes: database.prepare("SELECT rowid, * FROM session_nodes ORDER BY session_key").all(),
    windows: database.prepare("SELECT * FROM session_windows ORDER BY session_id").all(),
    transcript: database.prepare("SELECT rowid, * FROM transcript_events ORDER BY rowid").all(),
    snapshots: database
      .prepare("SELECT * FROM session_entry_snapshots ORDER BY session_key, field")
      .all(),
    members: database
      .prepare("SELECT * FROM session_members ORDER BY session_key, identity_id")
      .all(),
  };
}

it("migrates v24 creation surfaces atomically while preserving creators, snapshots, transcripts and sharing", async () => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    const pathname = state.path("dock-v24.sqlite");
    let database = new DatabaseSync(pathname);
    try {
      seedV24(database);
      const before = preservedRows(database);
      const createDock = () =>
        database.exec(`INSERT INTO session_nodes
        (session_key, current_session_id, entry_json, updated_at, created_via, created_actor_type, created_actor_id)
        VALUES ('agent:main:dock', 'dock-transcript',
          '{"sessionId":"dock-transcript","updatedAt":2,"createdVia":"plugin-dock","createdActor":{"type":"human","source":"profile","id":"creator"}}',
          2, 'plugin-dock', 'human', 'creator')`);
      expect(createDock).toThrow(/CHECK constraint/);
      const options = { agentId: "main", path: pathname, env: state.env };
      expect(() => ensureOpenClawAgentDatabaseSchema(database, options)).toThrow(
        /maintenance|lease/,
      );
      await withAgentDatabaseMaintenanceLease({ env: state.env }, async () => {
        ensureOpenClawAgentDatabaseSchema(database, options);
      });
      expect(database.prepare("PRAGMA user_version").get()).toEqual({
        user_version: OPENCLAW_AGENT_SCHEMA_VERSION,
      });
      expect(database.prepare("SELECT schema_version FROM schema_meta").get()).toEqual({
        schema_version: OPENCLAW_AGENT_SCHEMA_VERSION,
      });
      // Migration admission validates previously unadmitted canonical rows.
      for (const row of before.nodes) {
        row.entry_valid = 1;
      }
      expect(preservedRows(database)).toEqual(before);
      expect(database.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
      expect(database.prepare("SELECT * FROM fixture_session_names").all()).toEqual([
        { session_key: "agent:main:legacy" },
      ]);
      createDock();
      expect(
        database
          .prepare(
            "SELECT created_via, created_actor_type, created_actor_id FROM session_nodes WHERE session_key = 'agent:main:dock'",
          )
          .get(),
      ).toEqual({
        created_via: "plugin-dock",
        created_actor_type: "human",
        created_actor_id: "creator",
      });
      const migrated = preservedRows(database);
      database.close();
      database = new DatabaseSync(pathname);
      ensureOpenClawAgentDatabaseSchema(database, options);
      for (const row of migrated.nodes) {
        row.entry_valid = 1;
      }
      expect(preservedRows(database)).toEqual(migrated);
      database.exec("DELETE FROM fixture_session_names WHERE session_key = 'agent:main:legacy'");
      const deleted = preservedRows(database);
      expect(deleted.windows).toEqual([]);
      expect(deleted.transcript).toEqual([]);
      expect(deleted.snapshots).toEqual([]);
      expect(deleted.members).toEqual([]);
    } finally {
      database.close();
    }
  });
});

it("rolls back the creation-surface rebuild and version markers when publication is refused", async () => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    const pathname = state.path("dock-refused-v24.sqlite");
    const database = new DatabaseSync(pathname);
    try {
      seedV24(database);
      const before = {
        rows: preservedRows(database),
        schema: database.prepare("SELECT name, sql FROM sqlite_schema ORDER BY name").all(),
        metadata: database.prepare("SELECT * FROM schema_meta").all(),
      };
      let reachedPublication = false;
      database.setAuthorizer((action, name, value) => {
        if (
          action === constants.SQLITE_PRAGMA &&
          name === "user_version" &&
          value === String(OPENCLAW_AGENT_SCHEMA_VERSION)
        ) {
          reachedPublication = true;
          return constants.SQLITE_DENY;
        }
        return constants.SQLITE_OK;
      });
      await expect(
        withAgentDatabaseMaintenanceLease({ env: state.env }, async () => {
          ensureOpenClawAgentDatabaseSchema(database, {
            agentId: "main",
            path: pathname,
            env: state.env,
          });
        }),
      ).rejects.toThrow(/authoriz/i);
      database.setAuthorizer(null);
      expect(reachedPublication).toBe(true);
      expect(database.prepare("PRAGMA user_version").get()).toEqual({ user_version: 24 });
      expect(preservedRows(database)).toEqual(before.rows);
      expect(database.prepare("SELECT name, sql FROM sqlite_schema ORDER BY name").all()).toEqual(
        before.schema,
      );
      expect(database.prepare("SELECT * FROM schema_meta").all()).toEqual(before.metadata);
    } finally {
      database.setAuthorizer(null);
      database.close();
    }
  });
});
