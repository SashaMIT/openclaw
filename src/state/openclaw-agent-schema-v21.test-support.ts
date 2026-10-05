import fs from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { parseSqliteTableDefinition } from "../infra/sqlite-schema-contract-assembly.js";
import { extractSqliteTableSchema, quoteSqliteIdentifier } from "../infra/sqlite-schema-sql.js";
import { resolveOpenClawAgentSqlitePath } from "./openclaw-agent-db.paths.js";

// Frozen from f69617aa3818d805889692918ee7f51bef666597 as the original schema-21 migration input.
// Historical migration inputs must not inherit the current runtime's new column definitions.
export const OPENCLAW_AGENT_SCHEMA_V21_SQL = fs.readFileSync(
  new URL("../../test/fixtures/sqlite/openclaw-agent-schema-v21.sql", import.meta.url),
  "utf8",
);

export function seedOpenClawAgentSchemaV21(database: DatabaseSync, agentId = "main"): void {
  database.exec(OPENCLAW_AGENT_SCHEMA_V21_SQL);
  database.exec("PRAGMA user_version = 21");
  database
    .prepare(`INSERT INTO schema_meta
    (meta_key, role, schema_version, agent_id, app_version, created_at, updated_at)
    VALUES ('primary', 'agent', 21, ?, '2026.9.4', 1, 1)`)
    .run(agentId);
}

export function materializeV21WorkerAgentDatabase(stateDir: string): string {
  const databasePath = resolveOpenClawAgentSqlitePath({
    agentId: "worker-1",
    env: { OPENCLAW_STATE_DIR: stateDir },
  });
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });
  const database = new (requireNodeSqlite().DatabaseSync)(databasePath);
  try {
    seedOpenClawAgentSchemaV21(database, "worker-1");
  } finally {
    database.close();
  }
  return databasePath;
}

/** Preserve session/board setup while replacing unused compact storage with its frozen old shape. */
export function restoreEmptyV21StorageForHistoricalFixture(database: DatabaseSync): void {
  const tables = [
    "transcript_events",
    "memory_index_chunks",
    "memory_embedding_cache",
    "session_transcript_fts_rows",
    "session_entry_snapshots",
  ];
  for (const table of tables) {
    if (database.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get()) {
      throw new Error(`Historical fixture must not discard ${table} data`);
    }
  }
  const foreignKeys = database.prepare("PRAGMA foreign_keys").get()?.foreign_keys;
  database.exec("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE");
  try {
    for (const table of tables) {
      database.exec(`DROP TABLE ${table}`);
    }
    // Rebuild the node from frozen DDL so its CHECK and columns are historical too.
    const nodes = extractSqliteTableSchema(OPENCLAW_AGENT_SCHEMA_V21_SQL, "session_nodes");
    const columns = ["rowid", ...parseSqliteTableDefinition(nodes, "session_nodes").columns.keys()]
      .map(quoteSqliteIdentifier)
      .join(", ");
    const dependents = database
      .prepare(`SELECT type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL
        AND (type IN ('trigger', 'view') OR (type = 'index' AND tbl_name = 'session_nodes'))
        ORDER BY CASE type WHEN 'view' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name`)
      .all();
    for (const type of ["trigger", "view"]) {
      for (const dependent of dependents) {
        if (dependent.type === type && typeof dependent.name === "string") {
          database.exec(`DROP ${type.toUpperCase()} ${quoteSqliteIdentifier(dependent.name)}`);
        }
      }
    }
    database.exec(`
      ${nodes.replace("IF NOT EXISTS session_nodes", "session_nodes_v21_fixture")}
      INSERT INTO session_nodes_v21_fixture (${columns}) SELECT ${columns} FROM session_nodes;
      DROP TABLE session_nodes;
      ALTER TABLE session_nodes_v21_fixture RENAME TO session_nodes;
    `);
    database.exec(OPENCLAW_AGENT_SCHEMA_V21_SQL);
    for (const dependent of dependents) {
      if (
        typeof dependent.name === "string" &&
        typeof dependent.sql === "string" &&
        !database.prepare("SELECT 1 FROM sqlite_schema WHERE name = ?").get(dependent.name)
      ) {
        database.exec(dependent.sql);
      }
    }
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  } finally {
    if (foreignKeys === 1) {
      database.exec("PRAGMA foreign_keys = ON");
    }
  }
}
