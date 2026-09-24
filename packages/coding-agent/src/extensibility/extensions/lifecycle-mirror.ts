/**
 * Bridge from collab-guest mirrored session events to the local extension
 * runner.
 *
 * A collab guest renders host activity through the ordinary event pipeline,
 * but its own agent loop never runs, so `AgentSession`'s
 * `#emitExtensionEvent` path stays silent for the whole join. Lifecycle
 * integrations installed as extensions (e.g. Herdr's pane-state reporter)
 * therefore never see `agent_start`/`agent_end` and report a stale state for
 * the guest's pane. This module re-uses the same wire-event → extension-event
 * mapping `AgentSession` applies to its own events, so a guest's extension
 * handlers observe the same lifecycle transitions a local session would.
 */
import { logger } from "@oh-my-pi/pi-utils";
import type { AgentSessionEvent } from "../../session/agent-session";
import type {
	AgentEndEvent,
	AutoCompactionEndEvent,
	AutoCompactionStartEvent,
	AutoRetryEndEvent,
	AutoRetryStartEvent,
	MessageEndEvent,
	MessageStartEvent,
	MessageUpdateEvent,
	ToolExecutionEndEvent,
	ToolExecutionStartEvent,
	ToolExecutionUpdateEvent,
	TurnEndEvent,
	TurnStartEvent,
} from "./types";

/** Extension runner events a mirrored guest event can map onto. */
export type ExtensionRunnerEvent =
	| { type: "agent_start" }
	| AgentEndEvent
	| TurnStartEvent
	| TurnEndEvent
	| MessageStartEvent
	| MessageUpdateEvent
	| MessageEndEvent
	| ToolExecutionStartEvent
	| ToolExecutionUpdateEvent
	| ToolExecutionEndEvent
	| AutoCompactionStartEvent
	| AutoCompactionEndEvent
	| AutoRetryStartEvent
	| AutoRetryEndEvent;

/**
 * Map a mirrored host event onto the extension event `AgentSession` emits for
 * the same session event (see `#emitExtensionEvent` there; this duplicates
 * that mapping because the session's own copy is private and interleaved with
 * session-local state like `#turnIndex` — the mirrored stream carries the
 * wire's turn indices instead). Returns `null` for events with no extension
 * counterpart, so callers skip the runner entirely instead of emitting a
 * no-op.
 *
 * `agent_end` mirrors the session's public-notification shape
 * (`#emitAgentEndNotification`): `messages` plus the continuation flag. The
 * wire carries the core `agent_end` plus the session-layer `isTerminal` flag;
 * `isTerminal: false` marks a settle with a continuation already scheduled,
 * which maps to `willContinue: true`. Older hosts omit the flag and the
 * session emitted such settles without one — the absence maps to
 * `willContinue: undefined`, matching the pre-flag notification.
 */
export function extensionEventFromSessionEvent(
	event: AgentSessionEvent,
	turnIndex: number,
): ExtensionRunnerEvent | null {
	switch (event.type) {
		case "agent_start":
			return { type: "agent_start" };
		case "agent_end":
			return {
				type: "agent_end",
				messages: event.messages,
				willContinue: event.isTerminal === false ? true : undefined,
			};
		case "turn_start":
			return { type: "turn_start", turnIndex, timestamp: Date.now() };
		case "turn_end":
			return {
				type: "turn_end",
				turnIndex,
				message: event.message,
				toolResults: event.toolResults,
			};
		case "message_start":
			return { type: "message_start", message: event.message };
		case "message_update":
			return { type: "message_update", message: event.message, assistantMessageEvent: event.assistantMessageEvent };
		case "message_end":
			return { type: "message_end", message: event.message };
		case "tool_execution_start":
			return {
				type: "tool_execution_start",
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				args: event.args,
				intent: event.intent,
			};
		case "tool_execution_update":
			return {
				type: "tool_execution_update",
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				args: event.args,
				partialResult: event.partialResult,
			};
		case "tool_execution_end":
			return {
				type: "tool_execution_end",
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				result: event.result,
				isError: event.isError ?? false,
			};
		case "auto_compaction_start":
			return { type: "auto_compaction_start", reason: event.reason, action: event.action };
		case "auto_compaction_end":
			return {
				type: "auto_compaction_end",
				action: event.action,
				result: event.result,
				aborted: event.aborted,
				willRetry: event.willRetry,
				errorMessage: event.errorMessage,
			};
		case "auto_retry_start":
			return {
				type: "auto_retry_start",
				attempt: event.attempt,
				maxAttempts: event.maxAttempts,
				delayMs: event.delayMs,
				errorMessage: event.errorMessage,
			};
		case "auto_retry_end":
			return {
				type: "auto_retry_end",
				success: event.success,
				attempt: event.attempt,
				finalError: event.finalError,
			};
		default:
			return null;
	}
}

/**
 * Mirrors lifecycle-relevant collab guest events into an `ExtensionRunner`.
 *
 * Owns the per-join turn index the session maintains for its own extension
 * events (`#turnIndex`): the wire's turn_start/turn_end carry no counter, so
 * this class numbers turns itself exactly like `AgentSession` numbers its
 * own (zero-based, incremented after each `turn_end`). Emission is
 * fire-and-forget — a slow extension handler must not stall the guest's
 * frame-application chain.
 */
export class GuestLifecycleEmitter {
	#turnIndex = 0;

	emit(runner: { emit: (event: ExtensionRunnerEvent) => Promise<unknown> }, event: AgentSessionEvent): void {
		const mapped = extensionEventFromSessionEvent(event, this.#turnIndex);
		if (!mapped) return;
		if (event.type === "turn_end") this.#turnIndex++;
		void runner.emit(mapped).catch(err => {
			logger.warn("collab guest extension event emit failed", { type: event.type, error: String(err) });
		});
	}
}
