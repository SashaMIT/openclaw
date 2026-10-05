import "../test-utils/prepare-compiled-subprocesses.js";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { observeHostDataSql } from "../../test/helpers/sqlite-statement-execution-counter.js";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { makeUserMessage } from "../../test/helpers/user-message.js";
import { withSessionManagerIncognitoActor } from "../agents/sessions/session-manager-incognito-scope.js";
import { appendSessionTranscriptNote } from "../agents/sessions/session-manager-write-admission.js";
import { loadSessionEntryForAdmission } from "../config/sessions/session-accessor.sqlite-entry-admission.js";
import type { IncognitoSessionAuthority } from "../config/sessions/session-incognito-contract.js";
import {
  SessionReactionMessageMissingError,
  setSessionReactionAsync,
} from "../config/sessions/session-reaction-store.js";
import {
  claimHeartbeatOutcomeForRun,
  persistHeartbeatOutcome,
} from "../infra/heartbeat-outcome-store.js";
import * as workerStores from "../infra/sqlite-worker-store.js";
import { IncognitoSessionEndedError } from "../state/incognito-session-error.js";
import type { IncognitoAgentDatabaseExecution } from "../state/openclaw-agent-execution-incognito.js";
import { captureOpenClawAgentDatabaseExecution } from "../state/openclaw-agent-execution.js";
import { createIncognitoProgressCardStore } from "./progress-card-store.js";

const tempDirs = useAutoCleanupTempDirTracker(afterAll);
const authority: IncognitoSessionAuthority = { assertCurrent() {} };
let actor: IncognitoAgentDatabaseExecution;
let env: NodeJS.ProcessEnv;

beforeAll(async () => {
  env = { OPENCLAW_STATE_DIR: tempDirs.make("incognito-domain-facades-") };
  const opened = await captureOpenClawAgentDatabaseExecution({
    kind: "ephemeral",
    agentId: "main",
    env,
    authority,
  });
  assert(opened);
  actor = opened;
});
afterAll(async () => {
  await actor?.close();
});

async function fixture(name: string, source = authority) {
  const target = {
    agentId: "main",
    sessionKey: `agent:main:dashboard:incognito-${name}`,
    sessionId: name,
    storePath: actor.path,
    env,
  };
  const scope = { ...target, incognito: { actor, authority: source } };
  await actor.sessions.create(authority, {
    sessionKey: scope.sessionKey,
    entry: { sessionId: name, lifecycleRevision: "initial", updatedAt: 1, incognito: true },
  });
  const appended = await withSessionManagerIncognitoActor(actor, () =>
    appendSessionTranscriptNote(target, makeUserMessage("Private message", 1)),
  );
  assert(appended);
  const reaction = {
    messageId: appended.messageId,
    expectedSessionId: name,
    emoji: "👍",
    identityId: "viewer",
  };
  const store = createIncognitoProgressCardStore(() => scope);
  const heartbeat = {
    ...scope,
    runSessionKey: scope.sessionKey,
    response: { outcome: "progress" as const, summary: "Private progress", notify: false },
    occurredAt: 10,
  };
  return { scope, reaction, store, heartbeat };
}

it("composes reactions, heartbeat claims, and progress-card revisions without caller SQL", async () => {
  const { scope, reaction, store, heartbeat } = await fixture("composition");
  const sql = observeHostDataSql();
  try {
    expect(await setSessionReactionAsync(scope, reaction)).toMatchObject({ changed: true });
    await expect(
      setSessionReactionAsync(scope, { ...reaction, messageId: "missing" }),
    ).rejects.toBeInstanceOf(SessionReactionMessageMissingError);
    expect(
      await actor.sessions.sideData(authority, {
        type: "session.reactions.read",
        input: { sessionKey: scope.sessionKey, sessionId: scope.sessionId },
      }),
    ).toEqual({
      [reaction.messageId]: [{ emoji: "👍", count: 1, identities: [{ id: "viewer" }] }],
    });
    await persistHeartbeatOutcome(heartbeat);
    expect(await claimHeartbeatOutcomeForRun({ ...scope, runId: "first" })).toMatchObject({
      summary: "Private progress",
    });
    expect(await claimHeartbeatOutcomeForRun({ ...scope, runId: "first" })).toBeDefined();
    expect(await claimHeartbeatOutcomeForRun({ ...scope, runId: "second" })).toBeUndefined();
    expect(await store.put(scope.sessionKey, { markdown: "First" })).toMatchObject({
      card: { revision: 1, markdown: "First" },
    });
    expect(await store.put(scope.sessionKey, { markdown: "Second" })).toMatchObject({
      card: { revision: 2 },
    });
    expect(await store.put(scope.sessionKey, { expectedRevision: 1 })).toMatchObject({
      card: { revision: 2 },
    });
    expect(await store.get(scope.sessionKey)).toMatchObject({ markdown: "Second", revision: 2 });
    expect(await store.put(scope.sessionKey, { expectedRevision: 2 })).toEqual({ card: null });
    expect(await store.get(scope.sessionKey)).toBeNull();
    expect(sql.queries).toEqual([]);
    expect(existsSync(actor.path)).toBe(false);
  } finally {
    sql.restore();
  }
});

it("retains independent admission claims and revokes only the released claim", async () => {
  const { scope } = await fixture("admission");
  const sql = observeHostDataSql();
  const claims: Awaited<ReturnType<typeof loadSessionEntryForAdmission>>[] = [];
  try {
    claims.push(await loadSessionEntryForAdmission(scope, { incognito: scope.incognito }));
    claims.push(await loadSessionEntryForAdmission(scope, { incognito: scope.incognito }));
    const first = claims[0]!;
    const second = claims[1]!;
    expect(first.entry).toMatchObject({ sessionId: scope.sessionId });
    expect(first.databaseClaim.identity).toBe(second.databaseClaim.identity);
    expect(first.databaseClaim.incarnation).toBe(actor.identity.incarnation);
    await first.databaseClaim.release();
    expect(first.databaseClaim.isCurrent()).toBe(false);
    expect(second.databaseClaim.isCurrent()).toBe(true);
    actor.assertCurrent();
    await second.databaseClaim.release();
    expect(second.databaseClaim.isCurrent()).toBe(false);
    expect(sql.queries).toEqual([]);
  } finally {
    await Promise.all(claims.map(({ databaseClaim }) => Promise.resolve(databaseClaim.release())));
    sql.restore();
  }
});

it.each(["transaction", "commit"] as const)(
  "rolls domain mutations back when current authority refuses at %s",
  async (stage) => {
    let refused = true;
    const { scope, reaction, store, heartbeat } = await fixture(`refused-${stage}`, {
      assertCurrent() {},
      authorize(currentStage) {
        if (refused && currentStage === stage) {
          throw new Error("Domain authority revoked");
        }
      },
    });
    await expect(setSessionReactionAsync(scope, reaction)).rejects.toThrow(
      "Domain authority revoked",
    );
    await expect(persistHeartbeatOutcome(heartbeat)).rejects.toThrow("Domain authority revoked");
    await expect(store.put(scope.sessionKey, { markdown: "Refused" })).rejects.toThrow(
      "Domain authority revoked",
    );
    refused = false;
    expect(await store.get(scope.sessionKey)).toBeNull();
    expect(await claimHeartbeatOutcomeForRun({ ...scope, runId: "next" })).toBeUndefined();
    expect(await setSessionReactionAsync(scope, { ...reaction, remove: true })).toMatchObject({
      changed: false,
    });
  },
);

it("refuses progress-card disclosure when authorization ends after the worker read", async () => {
  let allowed = true;
  let reading = false;
  const { scope, store } = await fixture("read-revocation", {
    assertCurrent() {
      if (!allowed) {
        throw new Error("Card reader revoked");
      }
    },
    authorize(stage) {
      if (reading && stage === "commit") {
        allowed = false;
      }
    },
  });
  await store.put(scope.sessionKey, { markdown: "Private" });
  reading = true;
  await expect(store.get(scope.sessionKey)).rejects.toThrow("Card reader revoked");
});

it("keeps the actor transport alive through admission policy cleanup after close revokes the claim", async ({
  onTestFinished,
}) => {
  let nativeStore: { close(): Promise<void> } | undefined;
  const open = workerStores.openEphemeralAgentDatabaseSqliteWorkerStore;
  const opening = vi
    .spyOn(workerStores, "openEphemeralAgentDatabaseSqliteWorkerStore")
    .mockImplementation(async (...args) => {
      const store = await open(...args);
      nativeStore = store;
      return store;
    });
  onTestFinished(() => opening.mockRestore());
  const closingActor = await captureOpenClawAgentDatabaseExecution({
    kind: "ephemeral",
    agentId: "claim-close",
    env,
    authority,
  });
  opening.mockRestore();
  assert(closingActor && nativeStore);
  onTestFinished(() => closingActor.close());
  const scope = {
    agentId: closingActor.agentId,
    sessionKey: "agent:claim-close:dashboard:incognito-claim",
    storePath: closingActor.path,
    env,
    incognito: { actor: closingActor, authority },
  };
  await closingActor.sessions.create(authority, {
    sessionKey: scope.sessionKey,
    entry: { sessionId: "claim-close", updatedAt: 1, incognito: true },
  });
  const { databaseClaim } = await loadSessionEntryForAdmission(scope, {
    incognito: scope.incognito,
  });
  onTestFinished(() => databaseClaim.release());
  const closeTransport = vi.spyOn(nativeStore, "close");
  const closing = closingActor.close();
  try {
    expect(() => databaseClaim.assertCurrent()).toThrow(IncognitoSessionEndedError);
    expect(databaseClaim.isCurrent()).toBe(false);
    // A full microtask checkpoint lets close reach its transport unless the claim retains it.
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    expect(closeTransport).not.toHaveBeenCalled();
    const releasing = databaseClaim.release();
    expect(() => databaseClaim.assertCurrent()).toThrow("admission claim is released");
    await Promise.all([releasing, closing]);
    expect(closeTransport).toHaveBeenCalledOnce();
  } finally {
    await databaseClaim.release();
    await closing;
    closeTransport.mockRestore();
  }
});
