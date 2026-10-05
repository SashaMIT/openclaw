import { normalizeStructuredPromptSection } from "@openclaw/ai/internal/shared";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { joinPresentTextSegments } from "../../../shared/text/join-segments.js";
import type { isCacheTtlEligibleProvider } from "../cache-ttl.js";
import {
  persistToolResultProjections,
  type ToolResultPromptProjectionState,
} from "../session-prompt-state.js";

/**
 * Combines hook-provided system context with the base prompt while preserving
 * stable structured-section bytes. Returning undefined when hooks add nothing
 * lets callers avoid rewriting the original prompt.
 */
export function composeSystemPromptWithHookContext(params: {
  baseSystemPrompt?: string;
  prependSystemContext?: string;
  appendSystemContext?: string;
}): string | undefined {
  const prependSystem =
    typeof params.prependSystemContext === "string"
      ? normalizeStructuredPromptSection(params.prependSystemContext)
      : "";
  const appendSystem =
    typeof params.appendSystemContext === "string"
      ? normalizeStructuredPromptSection(params.appendSystemContext)
      : "";
  if (!prependSystem && !appendSystem) {
    return undefined;
  }
  return joinPresentTextSegments([prependSystem, params.baseSystemPrompt, appendSystem], {
    trim: true,
  });
}

/**
 * Returns the workspace path that must be mounted for sandboxed spawn attempts.
 * Read-only sandbox modes need the resolved workspace explicitly; full rw
 * access uses the normal workspace wiring.
 */
export function resolveAttemptSpawnWorkspaceDir(params: {
  sandbox?: {
    enabled?: boolean;
    workspaceAccess?: string;
  } | null;
  resolvedWorkspace: string;
}): string | undefined {
  return params.sandbox?.enabled && params.sandbox.workspaceAccess !== "rw"
    ? params.resolvedWorkspace
    : undefined;
}

/**
 * Appends the cache-TTL transcript marker when context-pruning policy and model
 * eligibility both allow it. The boolean result tells callers whether the
 * session transcript changed.
 */
export async function appendAttemptCacheTtlIfNeeded(params: {
  sessionManager: {
    appendCustomEntryAsync: (customType: string, data: unknown) => Promise<unknown>;
  };
  timedOutDuringCompaction: boolean;
  compactionOccurredThisAttempt: boolean;
  config?: OpenClawConfig;
  provider: string;
  modelId: string;
  modelApi?: string;
  modelRoute?: Parameters<typeof isCacheTtlEligibleProvider>[3];
  isCacheTtlEligibleProvider: typeof isCacheTtlEligibleProvider;
  now?: number;
  toolResultPromptProjectionState: ToolResultPromptProjectionState;
}): Promise<boolean> {
  // Compaction and timeout attempts already rewrite the transcript boundary.
  if (
    params.timedOutDuringCompaction ||
    params.compactionOccurredThisAttempt ||
    params.config?.agents?.defaults?.contextPruning?.mode !== "cache-ttl" ||
    !params.isCacheTtlEligibleProvider(
      params.provider,
      params.modelId,
      params.modelApi,
      params.modelRoute,
    )
  ) {
    return false;
  }
  await persistToolResultProjections(
    params.toolResultPromptProjectionState,
    (customType, data) => params.sessionManager.appendCustomEntryAsync(customType, data),
    { timestamp: params.now ?? Date.now(), provider: params.provider, modelId: params.modelId },
  );
  return true;
}
