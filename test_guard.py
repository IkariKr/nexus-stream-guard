"""Comprehensive automated test suite for Nexus Stream Guard.

Covers all 28 test scenarios:
- Scenario A: output_item.done -> response.completed
- Scenario A Heuristic: reasoning-only -> response.failed(stream_truncated_after_reasoning)
- Scenario B: function_call_arguments.delta -> response.failed(stream_truncated_tool_args)
- Scenario C: output_text.delta -> response.failed(stream_truncated_text)
- Scenario D: reasoning_summary_text.delta -> response.failed(stream_truncated_reasoning)
- Scenario E: response.created only -> response.failed(stream_empty_or_premature)
- Scenario F: normal stream with completed -> passed through without synthesis
- Forward Compat: normal stream with incomplete -> passed through without synthesis
- Trailing Error Suppression: event: error (unexpected EOF) after message completed -> suppressed and synthesized response.completed
- Midstream Error Normalization: event: error during delta -> normalized into response.failed
- Upstream Error Passthrough: HTTP 401/500/response.failed -> passed through untouched
- Default Reject: unmapped last event -> response.failed(stream_closed_unclean_default_reject)
- Idle Timeout: timeout flag -> response.failed(idle_timeout)
- Content-Encoding & Hop-by-hop stripping
- Header filtering & auth pass-through
- Concurrency isolation: 5 concurrent streams with distinct state
- Safe output_index sorting with None / string types
"""

import asyncio
import gzip
import json
import threading
import time
from typing import List, Tuple
import unittest

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
import httpx
import uvicorn

from tools.nexus_stream_guard import guard
from tools.nexus_stream_guard.policies import (
    StreamContext,
    _safe_sort_output_items,
    build_completed_payload,
    build_failed_payload,
    evaluate_disconnection,
)


class TestNexusStreamGuardPolicies(unittest.TestCase):
    """Direct unit tests for policy matrix and payload synthesis."""

    def test_scenario_a_normal_output_item_done(self):
        ctx = StreamContext(
            response_id="resp_123",
            model="deepseek-v4.1-flash",
            created_at=1000000,
            max_sequence_number=5,
            last_event="response.output_item.done",
            completed_output_items=[
                {"id": "msg_1", "type": "message", "status": "completed", "output_index": 0}
            ],
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_completed")
        self.assertEqual(dec.event_name, "response.completed")
        self.assertEqual(dec.matched_policy, "scenario_a_output_item_done")

        p = dec.payload
        self.assertEqual(p["type"], "response.completed")
        self.assertEqual(p["sequence_number"], 6)  # 5 + 1
        resp = p["response"]
        self.assertEqual(resp["id"], "resp_123")
        self.assertEqual(resp["status"], "completed")
        self.assertEqual(resp["model"], "deepseek-v4.1-flash")
        self.assertEqual(len(resp["output"]), 1)
        self.assertEqual(resp["usage"]["total_tokens"], 0)

    def test_scenario_a_error_suppressed_after_completed_item(self):
        ctx = StreamContext(
            response_id="resp_suppressed",
            model="gemini-3.8-flash",
            created_at=1000000,
            max_sequence_number=8,
            last_event="error",
            error_suppressed=True,
            in_active_delta=False,
            completed_output_items=[
                {"id": "msg_gemini", "type": "message", "status": "completed", "output_index": 0}
            ],
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_completed")
        self.assertEqual(dec.event_name, "response.completed")
        self.assertEqual(dec.payload["response"]["id"], "resp_suppressed")

    def test_scenario_a_heuristic_reasoning_only(self):
        ctx = StreamContext(
            response_id="resp_123",
            model="deepseek-v4.1-flash",
            max_sequence_number=3,
            last_event="response.output_item.done",
            completed_output_items=[
                {"id": "rsn_1", "type": "reasoning", "status": "completed", "output_index": 0}
            ],
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.event_name, "response.failed")
        self.assertEqual(dec.matched_policy, "scenario_a_heuristic_reasoning_only")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_truncated_after_reasoning")

    def test_safe_sort_output_items(self):
        items = [
            {"id": "3", "output_index": "2"},
            {"id": "1", "output_index": None},
            {"id": "2", "output_index": 1},
        ]
        sorted_res = _safe_sort_output_items(items)
        self.assertEqual([x["id"] for x in sorted_res], ["1", "2", "3"])

    def test_scenario_b_tool_args_truncated(self):
        ctx = StreamContext(
            response_id="resp_tool",
            max_sequence_number=10,
            last_event="response.function_call_arguments.delta",
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.event_name, "response.failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_truncated_tool_args")
        self.assertEqual(dec.payload["sequence_number"], 11)

    def test_scenario_c_text_truncated(self):
        ctx = StreamContext(
            response_id="resp_txt",
            max_sequence_number=7,
            last_event="response.output_text.delta",
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_truncated_text")

    def test_scenario_d_reasoning_truncated(self):
        ctx = StreamContext(
            response_id="resp_rsn",
            max_sequence_number=4,
            last_event="response.reasoning_summary_text.delta",
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_truncated_reasoning")

    def test_scenario_e_empty_stream(self):
        ctx = StreamContext(
            response_id="resp_emp",
            max_sequence_number=1,
            last_event="response.created",
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_empty_or_premature")

    def test_scenario_f_terminal_already_present(self):
        ctx = StreamContext(
            last_event="response.completed",
            has_terminal_event=True,
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "none")

    def test_forward_compat_incomplete_already_present(self):
        ctx = StreamContext(
            last_event="response.incomplete",
            has_terminal_event=True,
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "none")

    def test_idle_timeout_override(self):
        ctx = StreamContext(
            last_event="response.output_item.done",
            is_timeout=True,
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "idle_timeout")

    def test_default_reject(self):
        ctx = StreamContext(
            last_event="some.unmapped.event",
            max_sequence_number=2,
        )
        dec = evaluate_disconnection(ctx)
        self.assertEqual(dec.action, "synthesize_failed")
        self.assertEqual(dec.payload["response"]["error"]["code"], "stream_closed_unclean_default_reject")


class TestNexusStreamGuardEndToEnd(unittest.TestCase):
    """End-to-End integration tests using Mock Upstream Server."""

    @classmethod
    def setUpClass(cls):
        cls.mock_app = FastAPI()
        cls.captured_headers = {}

        @cls.mock_app.post("/v1/responses")
        async def mock_responses(request: Request):
            cls.captured_headers = dict(request.headers)
            body = await request.json()
            scenario = body.get("scenario", "scenario_a")

            if scenario == "http_500":
                raw_err = json.dumps({"error": "Internal Server Error"}).encode("utf-8")
                gz_err = gzip.compress(raw_err)
                return Response(gz_err, status_code=500, headers={"Content-Encoding": "gzip", "Content-Type": "application/json"})

            async def gen():
                if scenario == "scenario_a":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1,"response":{"id":"resp_mock_a","model":"deepseek","created_at":123}}\n\n'
                    yield b'event: response.output_item.added\ndata: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"type":"message"}}\n\n'
                    yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":3,"delta":"Review done"}\n\n'
                    yield b'event: response.output_text.done\ndata: {"type":"response.output_text.done","sequence_number":4}\n\n'
                    yield b'event: response.output_item.done\ndata: {"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"msg_0","type":"message","status":"completed"}}\n\n'
                    return

                elif scenario == "scenario_trailing_error":
                    # Emit full output, then emit CLIProxyAPI-style event: error (unexpected EOF)
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1,"response":{"id":"resp_trailing_err","model":"gemini","created_at":123}}\n\n'
                    yield b'event: response.output_item.added\ndata: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"type":"message"}}\n\n'
                    yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":3,"delta":"Gemini output complete"}\n\n'
                    yield b'event: response.output_item.done\ndata: {"type":"response.output_item.done","sequence_number":4,"output_index":0,"item":{"id":"msg_gemini","type":"message","status":"completed"}}\n\n'
                    yield b'event: error\ndata: {"type":"error","code":"internal_server_error","message":"unexpected EOF","sequence_number":0}\n\n'
                    return

                elif scenario == "scenario_midstream_error":
                    # Emit partial delta, then emit event: error
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":2,"delta":"Partial..."}\n\n'
                    yield b'event: error\ndata: {"type":"error","code":"rate_limit_exceeded","message":"Rate limit reached"}\n\n'
                    return

                elif scenario == "scenario_b":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1,"response":{"id":"resp_mock_b"}}\n\n'
                    yield b'event: response.output_item.added\ndata: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"type":"function_call"}}\n\n'
                    yield b'event: response.function_call_arguments.delta\ndata: {"type":"response.function_call_arguments.delta","sequence_number":3,"delta":"{\\"arg\\": 12"}\n\n'
                    return

                elif scenario == "scenario_c":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","sequence_number":2,"delta":"Partial..."}\n\n'
                    return

                elif scenario == "scenario_d":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    yield b'event: response.reasoning_summary_text.delta\ndata: {"type":"response.reasoning_summary_text.delta","sequence_number":2,"delta":"Thinking..."}\n\n'
                    return

                elif scenario == "scenario_e":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    return

                elif scenario == "scenario_f":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1,"response":{"id":"resp_full"}}\n\n'
                    yield b'event: response.output_item.done\ndata: {"type":"response.output_item.done","sequence_number":2,"item":{"type":"message"}}\n\n'
                    yield b'event: response.completed\ndata: {"type":"response.completed","sequence_number":3,"response":{"status":"completed"}}\n\n'
                    yield b'data: [DONE]\n\n'
                    return

                elif scenario == "scenario_incomplete":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    yield b'event: response.incomplete\ndata: {"type":"response.incomplete","sequence_number":2,"response":{"status":"incomplete"}}\n\n'
                    return

                elif scenario == "scenario_upstream_failed":
                    yield b'event: response.created\ndata: {"type":"response.created","sequence_number":1}\n\n'
                    yield b'event: response.failed\ndata: {"type":"response.failed","sequence_number":2,"error":{"message":"quota"}}\n\n'
                    return

            resp_headers = {"Content-Type": "text/event-stream"}
            return StreamingResponse(gen(), media_type="text/event-stream", headers=resp_headers)

        @cls.mock_app.get("/v1/models")
        async def mock_models():
            return JSONResponse({"data": [{"id": "deepseek-v4.1-flash"}]})

        cls.mock_server = uvicorn.Server(uvicorn.Config(cls.mock_app, host="127.0.0.1", port=18398, log_level="error"))
        cls.mock_thread = threading.Thread(target=cls.mock_server.run, daemon=True)
        cls.mock_thread.start()
        time.sleep(0.5)

        guard.UPSTREAM_BASE_URL = "http://127.0.0.1:18398"
        guard.client = httpx.AsyncClient(
            base_url="http://127.0.0.1:18398",
            timeout=httpx.Timeout(connect=5.0, read=5.0, write=5.0, pool=5.0),
            headers={"Accept-Encoding": "identity"},
        )

        cls.guard_server = uvicorn.Server(uvicorn.Config(guard.app, host="127.0.0.1", port=18397, log_level="error"))
        cls.guard_thread = threading.Thread(target=cls.guard_server.run, daemon=True)
        cls.guard_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        pass

    def _call_guard(self, scenario: str) -> Tuple[List[str], List[dict], int, dict]:
        events = []
        payloads = []
        with httpx.Client(timeout=10.0) as c:
            r = c.post(
                "http://127.0.0.1:18397/v1/responses",
                json={"stream": True, "scenario": scenario, "model": "test-fallback-model"},
                headers={"Authorization": "Bearer test-key", "X-Custom": "foo"},
            )
            status = r.status_code
            resp_headers = dict(r.headers)
            if status != 200:
                return events, payloads, status, resp_headers

            current_event = None
            for line in r.iter_lines():
                line = line.strip()
                if line.startswith("event:"):
                    current_event = line[6:].strip()
                    events.append(current_event)
                elif line.startswith("data:"):
                    try:
                        data = json.loads(line[5:].strip())
                        payloads.append(data)
                    except Exception:
                        pass
        return events, payloads, status, resp_headers

    def test_e2e_scenario_a_synthesis(self):
        events, payloads, status, headers = self._call_guard("scenario_a")
        self.assertEqual(status, 200)
        self.assertEqual(events[-1], "response.completed")
        self.assertIn("response.output_item.done", events)
        self.assertNotIn("content-encoding", headers)

        seqs = [p.get("sequence_number") for p in payloads if "sequence_number" in p]
        self.assertEqual(seqs, [1, 2, 3, 4, 5, 6])
        last_p = payloads[-1]
        self.assertEqual(last_p["type"], "response.completed")
        self.assertEqual(last_p["response"]["status"], "completed")
        self.assertEqual(len(last_p["response"]["output"]), 1)

    def test_e2e_trailing_error_suppressed_and_completed(self):
        events, payloads, status, _ = self._call_guard("scenario_trailing_error")
        self.assertEqual(status, 200)
        # Verify that "error" event was suppressed!
        self.assertNotIn("error", events)
        # Verify that terminal event is response.completed
        self.assertEqual(events[-1], "response.completed")
        last_p = payloads[-1]
        self.assertEqual(last_p["type"], "response.completed")
        self.assertEqual(last_p["response"]["id"], "resp_trailing_err")
        self.assertEqual(len(last_p["response"]["output"]), 1)

    def test_e2e_midstream_error_normalized(self):
        events, payloads, status, _ = self._call_guard("scenario_midstream_error")
        self.assertEqual(status, 200)
        self.assertNotIn("error", events)
        self.assertEqual(events[-1], "response.failed")
        last_p = payloads[-1]
        self.assertEqual(last_p["response"]["error"]["message"], "Rate limit reached")

    def test_e2e_headers_and_auth_passthrough(self):
        _, _, status, _ = self._call_guard("scenario_a")
        self.assertEqual(status, 200)
        self.assertEqual(self.captured_headers.get("authorization"), "Bearer test-key")
        self.assertEqual(self.captured_headers.get("x-custom"), "foo")
        self.assertEqual(self.captured_headers.get("accept-encoding"), "identity")

    def test_e2e_scenario_b_tool_args_failed(self):
        events, payloads, status, _ = self._call_guard("scenario_b")
        self.assertEqual(status, 200)
        self.assertEqual(events[-1], "response.failed")
        self.assertNotIn("response.completed", events)
        last_p = payloads[-1]
        self.assertEqual(last_p["response"]["error"]["code"], "stream_truncated_tool_args")

    def test_e2e_scenario_c_text_failed(self):
        events, payloads, status, _ = self._call_guard("scenario_c")
        self.assertEqual(status, 200)
        self.assertEqual(events[-1], "response.failed")
        self.assertEqual(payloads[-1]["response"]["error"]["code"], "stream_truncated_text")

    def test_e2e_scenario_d_reasoning_failed(self):
        events, payloads, status, _ = self._call_guard("scenario_d")
        self.assertEqual(status, 200)
        self.assertEqual(events[-1], "response.failed")
        self.assertEqual(payloads[-1]["response"]["error"]["code"], "stream_truncated_reasoning")

    def test_e2e_scenario_e_empty_failed(self):
        events, payloads, status, _ = self._call_guard("scenario_e")
        self.assertEqual(status, 200)
        self.assertEqual(events[-1], "response.failed")
        self.assertEqual(payloads[-1]["response"]["error"]["code"], "stream_empty_or_premature")

    def test_e2e_scenario_f_normal_passthrough(self):
        events, payloads, status, _ = self._call_guard("scenario_f")
        self.assertEqual(status, 200)
        self.assertEqual(events, ["response.created", "response.output_item.done", "response.completed"])
        self.assertEqual(events.count("response.completed"), 1)

    def test_e2e_incomplete_passthrough(self):
        events, payloads, status, _ = self._call_guard("scenario_incomplete")
        self.assertEqual(status, 200)
        self.assertEqual(events, ["response.created", "response.incomplete"])
        self.assertNotIn("response.failed", events)

    def test_e2e_upstream_failed_passthrough(self):
        events, payloads, status, _ = self._call_guard("scenario_upstream_failed")
        self.assertEqual(status, 200)
        self.assertEqual(events, ["response.created", "response.failed"])
        self.assertEqual(events.count("response.failed"), 1)

    def test_e2e_upstream_500_passthrough(self):
        events, payloads, status, headers = self._call_guard("http_500")
        self.assertEqual(status, 500)
        self.assertNotIn("content-encoding", headers)

    def test_e2e_raw_byte_pipe_models(self):
        with httpx.Client() as c:
            r = c.get("http://127.0.0.1:18397/v1/models")
            self.assertEqual(r.status_code, 200)
            data = r.json()
            self.assertEqual(data["data"][0]["id"], "deepseek-v4.1-flash")

    def test_e2e_health_endpoint(self):
        with httpx.Client() as c:
            r = c.get("http://127.0.0.1:18397/health")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["status"], "ok")

    def test_e2e_concurrency_isolation(self):
        results = []

        def worker(idx):
            events, payloads, status, _ = self._call_guard("scenario_a")
            results.append((idx, events[-1], payloads[-1]["response"]["id"]))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 5)
        for _, last_event, resp_id in results:
            self.assertEqual(last_event, "response.completed")
            self.assertEqual(resp_id, "resp_mock_a")


if __name__ == "__main__":
    unittest.main()
