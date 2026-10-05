"""Decision matrix and synthesis policies for stream interruption handling."""

from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class StreamContext:
    """Per-request stream tracking context."""
    response_id: str = ""
    model: str = ""
    created_at: int = 0
    max_sequence_number: int = 0
    last_event: Optional[str] = None
    completed_output_items: List[Dict[str, Any]] = field(default_factory=list)
    has_terminal_event: bool = False
    is_timeout: bool = False
    item_types_seen: Set[str] = field(default_factory=set)
    in_active_delta: bool = False
    error_suppressed: bool = False

    # Active item tracking for midstream auto-seal
    active_output_item: Optional[Dict[str, Any]] = None
    active_output_index: int = 0
    active_text_deltas: List[str] = field(default_factory=list)

    def has_substantive_closed_output(self) -> bool:
        """Check if substantive output items (message/function_call) are already completed."""
        closed_types = {item.get("type") for item in self.completed_output_items}
        return bool(closed_types & {"message", "function_call", "custom_tool_call"})

    def has_active_text_output(self) -> bool:
        """Check if substantive non-empty text has been received for the active output item."""
        return bool("".join(self.active_text_deltas).strip())

    def get_accumulated_text(self) -> str:
        """Return full text accumulated so far for the active output item."""
        return "".join(self.active_text_deltas)

    def record_event(self, event_name: str, payload_dict: Dict[str, Any]) -> None:
        """Update stream tracking state upon receiving an SSE event."""
        self.last_event = event_name
        seq = payload_dict.get("sequence_number")
        if isinstance(seq, int) and seq > self.max_sequence_number:
            self.max_sequence_number = seq

        if event_name in ("response.completed", "response.failed", "response.incomplete"):
            self.has_terminal_event = True

        if event_name == "response.created":
            resp_obj = payload_dict.get("response", {})
            self.response_id = resp_obj.get("id", self.response_id)
            self.model = resp_obj.get("model", self.model)
            self.created_at = resp_obj.get("created_at", self.created_at)

        elif event_name == "response.output_item.added":
            self.in_active_delta = True
            item = payload_dict.get("item", {})
            self.active_output_item = item if isinstance(item, dict) else {}
            idx = payload_dict.get("output_index", 0)
            self.active_output_index = idx if isinstance(idx, int) else 0
            self.active_text_deltas = []
            itype = item.get("type") if isinstance(item, dict) else None
            if itype:
                self.item_types_seen.add(itype)

        elif event_name == "response.output_text.delta":
            self.in_active_delta = True
            delta_str = payload_dict.get("delta")
            if isinstance(delta_str, str):
                self.active_text_deltas.append(delta_str)

        elif event_name in ("response.function_call_arguments.delta", "response.reasoning_summary_text.delta"):
            self.in_active_delta = True

        elif event_name == "response.output_item.done":
            self.in_active_delta = False
            item = payload_dict.get("item")
            if isinstance(item, dict):
                self.completed_output_items.append(item)
                itype = item.get("type")
                if itype:
                    self.item_types_seen.add(itype)
            self.active_output_item = None
            self.active_text_deltas = []


@dataclass
class PolicyDecision:
    action: str  # "none", "synthesize_completed", "synthesize_failed"
    event_name: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    matched_policy: str = ""
    prefix_events: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)



def _safe_sort_output_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort output items safely handling missing or non-integer output_index."""
    def sort_key(item: Dict[str, Any]) -> int:
        idx = item.get("output_index")
        if isinstance(idx, int):
            return idx
        try:
            return int(idx)
        except (ValueError, TypeError):
            return 0
    return sorted(items, key=sort_key)


def build_completed_payload(ctx: StreamContext) -> Dict[str, Any]:
    """Construct an OpenAI Responses API compliant response.completed event."""
    seq = ctx.max_sequence_number + 1
    resp_id = ctx.response_id or f"resp_synth_{int(time.time() * 1000)}"
    created = ctx.created_at or int(time.time())
    model = ctx.model or "unknown"
    sorted_items = _safe_sort_output_items(ctx.completed_output_items)

    return {
        "type": "response.completed",
        "sequence_number": seq,
        "response": {
            "id": resp_id,
            "object": "response",
            "created_at": created,
            "status": "completed",
            "background": False,
            "error": None,
            "model": model,
            "output": sorted_items,
            "usage": {
                "input_tokens": 0,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 0,
                "total_tokens": 0,
                "output_tokens_details": {"reasoning_tokens": 0},
            },
        },
    }


def build_failed_payload(ctx: StreamContext, code: str, message: str) -> Dict[str, Any]:
    """Construct an OpenAI Responses API compliant response.failed event."""
    seq = ctx.max_sequence_number + 1
    resp_id = ctx.response_id or f"resp_synth_{int(time.time() * 1000)}"
    created = ctx.created_at or int(time.time())
    model = ctx.model or "unknown"
    sorted_items = _safe_sort_output_items(ctx.completed_output_items)

    return {
        "type": "response.failed",
        "sequence_number": seq,
        "response": {
            "id": resp_id,
            "object": "response",
            "created_at": created,
            "status": "failed",
            "background": False,
            "error": {
                "message": message,
                "type": "upstream_stream_error",
                "code": code,
            },
            "model": model,
            "output": sorted_items,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            },
        },
    }


def seal_active_text_output(ctx: StreamContext) -> Tuple[List[Tuple[str, Dict[str, Any]]], Dict[str, Any]]:
    """Synthesize sealing events for midstream truncated text output item.
    
    Generates:
    1. response.output_text.done (with full accumulated text)
    2. response.content_part.done
    3. response.output_item.done (with completed message object)
    
    Returns:
        (prefix_events_to_emit, completed_item_dict)
    """
    accumulated_text = ctx.get_accumulated_text()
    active_item = ctx.active_output_item or {}
    item_id = active_item.get("id") or f"msg_synth_{int(time.time() * 1000)}"
    output_idx = ctx.active_output_index
    
    seq = ctx.max_sequence_number + 1
    text_done_payload = {
        "type": "response.output_text.done",
        "sequence_number": seq,
        "output_index": output_idx,
        "item_id": item_id,
        "content_index": 0,
        "text": accumulated_text,
    }
    
    seq += 1
    part_done_payload = {
        "type": "response.content_part.done",
        "sequence_number": seq,
        "output_index": output_idx,
        "item_id": item_id,
        "content_index": 0,
        "part": {
            "type": "output_text",
            "text": accumulated_text,
            "annotations": [],
        },
    }
    
    completed_item = {
        "id": item_id,
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [
            {
                "type": "output_text",
                "text": accumulated_text,
                "annotations": [],
            }
        ],
        "output_index": output_idx,
    }
    
    seq += 1
    item_done_payload = {
        "type": "response.output_item.done",
        "sequence_number": seq,
        "output_index": output_idx,
        "item": completed_item,
    }
    
    # Update context sequence and completed items
    ctx.max_sequence_number = seq
    ctx.completed_output_items.append(completed_item)
    ctx.in_active_delta = False
    ctx.active_output_item = None
    ctx.active_text_deltas = []
    
    prefix_events = [
        ("response.output_text.done", text_done_payload),
        ("response.content_part.done", part_done_payload),
        ("response.output_item.done", item_done_payload),
    ]
    return prefix_events, completed_item



def evaluate_disconnection(ctx: StreamContext, auto_seal_text: bool = True) -> PolicyDecision:
    """Evaluate stream state upon upstream connection closure and determine synthesis action.
    
    Priority Rules:
    1. If a terminal event was already received (completed/failed/incomplete), do nothing.
    2. If disconnection is caused by idle timeout, always synthesize response.failed(idle_timeout).
    3. Scenario A: last_event == response.output_item.done OR trailing error suppressed ->
       Apply heuristic: if only reasoning items are closed and no message or function_call
       is closed, do NOT synthesize completed; synthesize response.failed(stream_truncated_after_reasoning).
       Otherwise, synthesize response.completed.
    4. Scenario B (tool args truncated): function_call_arguments.delta/added -> response.failed(stream_truncated_tool_args).
    5. Scenario C (text truncated):
       - If auto_seal_text is True and substantial text was accumulated:
         Seal the active output text item (emit output_text.done -> content_part.done -> output_item.done)
         and synthesize response.completed so client/subagents can preserve output without crashing.
       - Otherwise, synthesize response.failed(stream_truncated_text).
    6. Scenario D (reasoning truncated): reasoning_summary_text.delta/added -> response.failed(stream_truncated_reasoning).
    7. Scenario E (empty/premature): response.created/in_progress or no event -> response.failed(stream_empty_or_premature).
    8. Default Reject: any unmapped state -> response.failed(stream_closed_unclean_default_reject).
    """
    if ctx.has_terminal_event:
        return PolicyDecision(action="none", matched_policy="terminal_already_present")

    # Priority 2: Timeout override
    if ctx.is_timeout:
        payload = build_failed_payload(
            ctx,
            code="idle_timeout",
            message="Stream idle read timeout reached on proxy (>=600s)"
        )
        return PolicyDecision(
            action="synthesize_failed",
            event_name="response.failed",
            payload=payload,
            matched_policy="idle_timeout_override"
        )

    last_event = ctx.last_event

    # Priority 3: Scenario A (output_item.done OR trailing EOF error suppressed after closed substantive item)
    if last_event == "response.output_item.done" or (ctx.error_suppressed and ctx.has_substantive_closed_output() and not ctx.in_active_delta):
        has_substantive = ctx.has_substantive_closed_output()
        closed_types = {item.get("type") for item in ctx.completed_output_items}
        only_reasoning = ("reasoning" in closed_types) and not has_substantive

        if only_reasoning:
            payload = build_failed_payload(
                ctx,
                code="stream_truncated_after_reasoning",
                message="Stream disconnected after reasoning output item without substantive response item"
            )
            return PolicyDecision(
                action="synthesize_failed",
                event_name="response.failed",
                payload=payload,
                matched_policy="scenario_a_heuristic_reasoning_only"
            )

        payload = build_completed_payload(ctx)
        return PolicyDecision(
            action="synthesize_completed",
            event_name="response.completed",
            payload=payload,
            matched_policy="scenario_a_output_item_done"
        )

    # Priority 4: Scenario B (Tool Call argument truncation)
    if last_event in ("response.function_call_arguments.delta", "response.function_call_arguments.added"):
        payload = build_failed_payload(
            ctx,
            code="stream_truncated_tool_args",
            message="Upstream connection closed abruptly during function call arguments generation"
        )
        return PolicyDecision(
            action="synthesize_failed",
            event_name="response.failed",
            payload=payload,
            matched_policy="scenario_b_tool_args_truncated"
        )

    # Priority 5: Scenario C (Text truncation)
    if last_event in ("response.output_text.delta", "response.output_item.added", "response.output_text.done", "response.content_part.done"):
        if auto_seal_text and ctx.has_active_text_output():
            prefix_events, _ = seal_active_text_output(ctx)
            payload = build_completed_payload(ctx)
            return PolicyDecision(
                action="synthesize_completed",
                event_name="response.completed",
                payload=payload,
                matched_policy="scenario_c_auto_seal_text",
                prefix_events=prefix_events,
            )

        payload = build_failed_payload(
            ctx,
            code="stream_truncated_text",
            message="Upstream connection closed abruptly during text generation"
        )
        return PolicyDecision(
            action="synthesize_failed",
            event_name="response.failed",
            payload=payload,
            matched_policy="scenario_c_text_truncated"
        )

    # Priority 6: Scenario D (Reasoning truncation)
    if last_event in ("response.reasoning_summary_text.delta", "response.reasoning_summary_text.done", "response.reasoning.added"):
        payload = build_failed_payload(
            ctx,
            code="stream_truncated_reasoning",
            message="Upstream connection closed abruptly during reasoning phase"
        )
        return PolicyDecision(
            action="synthesize_failed",
            event_name="response.failed",
            payload=payload,
            matched_policy="scenario_d_reasoning_truncated"
        )

    # Priority 7: Scenario E (Empty / premature disconnect)
    if last_event in ("response.created", "response.in_progress") or not last_event:
        payload = build_failed_payload(
            ctx,
            code="stream_empty_or_premature",
            message="Upstream connection closed before generating any substantive stream chunks"
        )
        return PolicyDecision(
            action="synthesize_failed",
            event_name="response.failed",
            payload=payload,
            matched_policy="scenario_e_empty_or_premature"
        )

    # Priority 8: Default Reject fallback
    payload = build_failed_payload(
        ctx,
        code="stream_closed_unclean_default_reject",
        message=f"Upstream stream closed uncleanly with unmapped event state: {last_event}"
    )
    return PolicyDecision(
        action="synthesize_failed",
        event_name="response.failed",
        payload=payload,
        matched_policy="default_reject"
    )
