import type { DatabaseSync } from "node:sqlite";
import { normalizeOptionalLowercaseString } from "@openclaw/normalization-core/string-coerce";
import { sql } from "kysely";
import { executeSqliteQuerySync } from "../../infra/kysely-sync.js";
import { isIncognitoSessionKey, normalizeAgentId } from "../../routing/session-key.js";
import type { OpenClawStateReadCommand } from "../../state/openclaw-state-read.types.js";
import {
  acpSessionRowMatchesEntry,
  selectAcpSessionRows,
  selectAcpSessionRowsByKeys,
  buildAcpDatabaseSessionKey,
  getAcpSessionKysely,
  parseAcpDatabaseSessionKey,
} from "./session-meta-keys.js";

export type AcpResumeSessionRow = {
  sessionKey: string;
  session_id: string | null;
  updated_at: number;
};

function selectAcpResumeSessions(
  db: DatabaseSync,
  input: { agentId: string; backendId?: string; resumeSessionId: string; sessionKey?: string },
): AcpResumeSessionRow[] {
  // Match String.trim(), including historical whitespace, without decoding metadata on the host.
  // These expressions must match the canonical resume indexes.
  const whitespace = sql`char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)`;
  const identity = sql`CASE WHEN json_valid(identity_json) THEN identity_json END`;
  const agentSessionId = sql`trim(json_extract(${identity}, '$.agentSessionId'), ${whitespace})`;
  const acpxSessionId = sql`trim(json_extract(${identity}, '$.acpxSessionId'), ${whitespace})`;
  const rows = executeSqliteQuerySync(
    db,
    getAcpSessionKysely(db)
      .selectFrom("acp_sessions")
      .select(["session_key", "session_id", "updated_at", "backend"])
      .$if(input.sessionKey !== undefined, (query) =>
        query.where(
          "session_key",
          "=",
          buildAcpDatabaseSessionKey(input.sessionKey!, input.agentId),
        ),
      )
      .where((eb) =>
        eb.or([
          eb.and([
            eb(agentSessionId, "=", input.resumeSessionId),
            eb(sql`json_type(${identity}, '$.agentSessionId')`, "=", "text"),
          ]),
          eb.and([
            eb(acpxSessionId, "=", input.resumeSessionId),
            eb(sql`json_type(${identity}, '$.acpxSessionId')`, "=", "text"),
          ]),
        ]),
      ),
  ).rows;
  const agentId = normalizeAgentId(input.agentId);
  const backendId = normalizeOptionalLowercaseString(input.backendId);
  return rows
    .flatMap((row) => {
      const key = parseAcpDatabaseSessionKey(row.session_key);
      return key?.agentId === agentId &&
        !isIncognitoSessionKey(key.storeSessionKey) &&
        (!backendId || normalizeOptionalLowercaseString(row.backend) === backendId)
        ? [
            {
              sessionKey: key.storeSessionKey,
              session_id: row.session_id,
              updated_at: row.updated_at,
            },
          ]
        : [];
    })
    .sort((a, b) => Buffer.compare(Buffer.from(a.sessionKey), Buffer.from(b.sessionKey)));
}

export function readAcpSessionCommand(
  db: DatabaseSync,
  command: Extract<
    OpenClawStateReadCommand,
    {
      type: "acpSessions.list" | "acpSessions.metadata" | "acpSessions.resume";
    }
  >,
) {
  if (command.type === "acpSessions.list") {
    return { type: command.type, rows: selectAcpSessionRows(db) };
  }
  if (command.type === "acpSessions.resume") {
    return { type: command.type, rows: selectAcpResumeSessions(db, command) };
  }
  const cohortKeys = [...new Set(command.entries.flatMap((entry) => entry.keys))];
  const rows = new Map(
    [...selectAcpSessionRowsByKeys(db, cohortKeys)].map((row) => [row.session_key, row]),
  );
  return {
    type: command.type,
    rows: command.entries.map(
      ({ keys, entry }) =>
        keys
          .map((key) => rows.get(key))
          .find((row) => row && (!entry || acpSessionRowMatchesEntry(row, entry))) ?? null,
    ),
  };
}
