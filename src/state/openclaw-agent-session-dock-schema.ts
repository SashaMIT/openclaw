import type { DatabaseSync } from "node:sqlite";
import { parseSqliteTableDefinition } from "../infra/sqlite-schema-contract-assembly.js";
import { extractSqliteTableSchema, quoteSqliteIdentifier } from "../infra/sqlite-schema-sql.js";

export const SESSION_DOCK_CREATION_SCHEMA_VERSION = 25;

/** Older readers admit only the creation surfaces known to their schema. */
export function withoutSessionDockCreationSchema(schema: string): string {
  return schema.replace("'plugin', 'plugin-dock', 'internal'", "'plugin', 'internal'");
}

/** Rebuild only the node CHECK under the existing stopped-writer migration transaction. */
export function migrateSessionDockCreationSchemaInTransaction(
  database: DatabaseSync,
  schema: string,
): void {
  const canonical = extractSqliteTableSchema(schema, "session_nodes");
  const source = database
    .prepare("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'session_nodes'")
    .get();
  const definition = parseSqliteTableDefinition(
    typeof source?.sql === "string" ? source.sql : null,
    "session_nodes",
  );
  const createdVia = parseSqliteTableDefinition(canonical, "session_nodes").columns.get(
    "created_via",
  );
  if (!createdVia) {
    throw new Error("Canonical session creation-surface column is missing");
  }
  if (definition.columns.get("created_via") !== withoutSessionDockCreationSchema(createdVia)) {
    throw new Error("Session creation-surface migration requires the canonical legacy CHECK");
  }
  definition.columns.set("created_via", createdVia);
  const rowid = ["_rowid_", "rowid", "oid"].find((name) => !definition.columns.has(name));
  if (!rowid) {
    throw new Error("Session creation-surface migration cannot preserve shadowed rowids");
  }
  const columns = [rowid, ...definition.columns.keys()].map(quoteSqliteIdentifier).join(", ");
  const objects = database
    .prepare(`SELECT type, name, sql FROM sqlite_schema
      WHERE sql IS NOT NULL AND (type IN ('trigger', 'view')
        OR (type = 'index' AND tbl_name = 'session_nodes'))
      ORDER BY CASE type WHEN 'view' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name`)
    .all();
  // ALTER TABLE reparses unrelated triggers and views while the old table is absent.
  for (const type of ["trigger", "view"]) {
    for (const object of objects) {
      if (object.type === type && typeof object.name === "string") {
        database.exec(`DROP ${type.toUpperCase()} ${quoteSqliteIdentifier(object.name)}`);
      }
    }
  }
  database.exec(`
    CREATE TABLE session_nodes_dock_migration (
      ${[...definition.columns.values(), ...definition.constraints].join(",\n")}
    ) STRICT;
    INSERT INTO session_nodes_dock_migration (${columns}) SELECT ${columns} FROM session_nodes;
    DROP TABLE session_nodes;
    ALTER TABLE session_nodes_dock_migration RENAME TO session_nodes;
  `);
  for (const object of objects) {
    if (typeof object.sql === "string") {
      database.exec(object.sql);
    }
  }
}
