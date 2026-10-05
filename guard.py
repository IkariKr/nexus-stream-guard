"""Nexus Stream Guard Server.

A resilient, zero-overhead reverse proxy for OpenAI Responses API streaming.
Safely detects abrupt upstream disconnections and injects compliant terminal events
to keep ZCode/Codex Subagents from crashing.
"""

import json
import logging
import os
import sys
from typing import AsyncGenerator, Dict, Set

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import httpx
import uvicorn

from .policies import StreamContext, evaluate_disconnection

# Hop-by-hop & compression headers to filter out between proxies
HOP_BY_HOP_HEADERS: Set[str] = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "host",
    "content-encoding",  # Prevent downstream gunzip mismatch when body is already decoded
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [NexusStreamGuard] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("NexusStreamGuard")

UPSTREAM_BASE_URL = os.getenv("UPSTREAM_BASE_URL", "https://nexus.ikarikore.top")
UPSTREAM_VERIFY = os.getenv("UPSTREAM_VERIFY", "true").lower() in ("true", "1", "yes")
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "600.0"))

app = FastAPI(title="Nexus Stream Guard", version="1.0.0")

client = httpx.AsyncClient(
    base_url=UPSTREAM_BASE_URL,
    verify=UPSTREAM_VERIFY,
    timeout=httpx.Timeout(connect=20.0, read=READ_TIMEOUT, write=60.0, pool=READ_TIMEOUT),
    limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
    headers={"Accept-Encoding": "identity"},  # Request uncompressed to avoid downstream mismatches
)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await client.aclose()


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint for Docker / Komodo monitoring."""
    return {
        "status": "ok",
        "service": "cliproxyapi-stream-guard",
        "version": "1.0.0",
        "upstream": UPSTREAM_BASE_URL,
    }


def filter_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Clean headers by stripping hop-by-hop and encoding headers."""
    return {k.lower(): v for k, v in headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def proxy_router(request: Request, path: str):
    """Route requests: streaming /v1/responses is monitored; all else is raw byte passed."""
    upstream_path = f"/{path}"
    req_headers = filter_headers(dict(request.headers))
    req_headers["accept-encoding"] = "identity"
    body = await request.body()

    is_responses_endpoint = (
        upstream_path.rstrip("/").endswith("v1/responses") or upstream_path.rstrip("/") == "/responses"
    )
    is_streaming = False
    requested_model = ""

    if is_responses_endpoint and request.method == "POST":
        try:
            parsed = json.loads(body.decode("utf-8"))
            if isinstance(parsed, dict):
                if parsed.get("stream") is True:
                    is_streaming = True
                requested_model = str(parsed.get("model", ""))
        except Exception:
            pass

    # Non-streaming or other endpoints: 100% Raw Byte Pipe
    if not is_streaming:
        try:
            upstream_req = client.build_request(
                method=request.method,
                url=upstream_path,
                headers=req_headers,
                params=request.query_params,
                content=body,
            )
            upstream_resp = await client.send(upstream_req, stream=True)
        except Exception as e:
            logger.error(f"Non-stream connection error to {upstream_path}: {type(e).__name__}: {e}")
            return Response(
                content=json.dumps({"error": {"message": f"Upstream connection failed: {e}", "type": "proxy_error"}}),
                status_code=502,
                media_type="application/json",
            )

        resp_headers = filter_headers(dict(upstream_resp.headers))

        async def raw_body_generator() -> AsyncGenerator[bytes, None]:
            try:
                async for chunk in upstream_resp.aiter_raw():
                    yield chunk
            finally:
                await upstream_resp.aclose()

        return StreamingResponse(
            raw_body_generator(),
            status_code=upstream_resp.status_code,
            headers=resp_headers,
        )

    # Monitored streaming /v1/responses
    return await handle_streaming_responses(request, upstream_path, req_headers, body, requested_model)


async def handle_streaming_responses(
    request: Request, upstream_path: str, req_headers: Dict[str, str], body: bytes, requested_model: str = ""
) -> Response:
    upstream_req = client.build_request(
        method="POST",
        url=upstream_path,
        headers=req_headers,
        params=request.query_params,
        content=body,
    )

    try:
        upstream_resp = await client.send(upstream_req, stream=True)
    except Exception as e:
        logger.error(f"Failed to connect to upstream {upstream_path}: {type(e).__name__}: {e}")
        return Response(
            content=json.dumps({"error": {"message": f"Upstream connection failed: {e}", "type": "proxy_error"}}),
            status_code=502,
            media_type="application/json",
        )

    # If upstream returns non-200 (e.g. 401, 400, 429, 500), pass through directly
    if upstream_resp.status_code != 200:
        content = await upstream_resp.aread()
        resp_headers = filter_headers(dict(upstream_resp.headers))
        return Response(content=content, status_code=upstream_resp.status_code, headers=resp_headers)

    # If Content-Type is not event-stream, pass through
    content_type = upstream_resp.headers.get("content-type", "")
    if "text/event-stream" not in content_type:
        content = await upstream_resp.aread()
        resp_headers = filter_headers(dict(upstream_resp.headers))
        return Response(content=content, status_code=upstream_resp.status_code, headers=resp_headers)

    async def sse_event_guard_generator() -> AsyncGenerator[bytes, None]:
        ctx = StreamContext(model=requested_model)
        current_event_name = ""
        current_chunk_lines = []
        buffered_done_block: bytes = b""

        try:
            async for raw_line in upstream_resp.aiter_lines():
                line = raw_line.strip()
                if not line:
                    if current_chunk_lines:
                        block = "\n".join(current_chunk_lines) + "\n\n"
                        # If block is solely [DONE], buffer it to output after any synthetic terminal event
                        if len(current_chunk_lines) == 1 and current_chunk_lines[0] == "data: [DONE]":
                            buffered_done_block = block.encode("utf-8")
                        else:
                            yield block.encode("utf-8")
                        current_chunk_lines = []
                        current_event_name = ""  # Reset event name per event block
                    continue

                current_chunk_lines.append(line)

                if line.startswith("event:"):
                    current_event_name = line[6:].strip()
                elif line.startswith("data:"):
                    payload_str = line[5:].strip()
                    if payload_str and payload_str != "[DONE]":
                        try:
                            payload_json = json.loads(payload_str)
                            ctx.record_event(current_event_name, payload_json)
                        except Exception:
                            pass

            if current_chunk_lines:
                block = "\n".join(current_chunk_lines) + "\n\n"
                if len(current_chunk_lines) == 1 and current_chunk_lines[0] == "data: [DONE]":
                    buffered_done_block = block.encode("utf-8")
                else:
                    yield block.encode("utf-8")
                current_chunk_lines = []

        except httpx.ReadTimeout:
            logger.warning(f"Upstream read timeout ({READ_TIMEOUT}s) reached on stream {ctx.response_id or 'unknown'}")
            ctx.is_timeout = True
        except Exception as e:
            logger.warning(
                f"Upstream stream disconnected unexpectedly ({type(e).__name__}): {e} "
                f"[last_event={ctx.last_event}, resp_id={ctx.response_id}]"
            )
        finally:
            await upstream_resp.aclose()

        # Evaluate policy and synthesize terminal event if stream closed uncleanly
        try:
            decision = evaluate_disconnection(ctx)
            if decision.action != "none" and decision.event_name and decision.payload:
                synth_block = f"event: {decision.event_name}\ndata: {json.dumps(decision.payload)}\n\n".encode("utf-8")
                logger.info(
                    f"Stream closed without terminal event. Policy '{decision.matched_policy}' triggered -> "
                    f"Injected {decision.event_name} (resp_id={ctx.response_id}, items={len(ctx.completed_output_items)})"
                )
                yield synth_block
        except Exception as eval_err:
            logger.error(f"Error evaluating disconnection policy: {eval_err}", exc_info=True)
            # Final fallback to guarantee terminal event
            fallback_payload = {
                "type": "response.failed",
                "sequence_number": ctx.max_sequence_number + 1,
                "response": {
                    "id": ctx.response_id or "resp_fallback_err",
                    "status": "failed",
                    "error": {"message": f"Proxy evaluation error: {eval_err}", "type": "proxy_internal_error", "code": "proxy_eval_failed"},
                    "output": [],
                    "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
                }
            }
            yield f"event: response.failed\ndata: {json.dumps(fallback_payload)}\n\n".encode("utf-8")

        # Emit buffered [DONE] after terminal event if present
        if buffered_done_block:
            yield buffered_done_block

    resp_headers = filter_headers(dict(upstream_resp.headers))
    resp_headers["content-type"] = "text/event-stream"
    resp_headers["cache-control"] = "no-cache"
    resp_headers["x-stream-guard"] = "active"

    return StreamingResponse(sse_event_guard_generator(), headers=resp_headers, media_type="text/event-stream")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "18318"))
    host = os.getenv("HOST", "127.0.0.1")
    uvicorn.run("tools.nexus_stream_guard.guard:app", host=host, port=port, log_level="info")
