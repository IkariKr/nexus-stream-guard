# Nexus Stream Guard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)](https://fastapi.tiangolo.com/)

A lightweight, zero-overhead self-healing proxy middleware designed for the **OpenAI Responses API (`/v1/responses`)**. It prevents abrupt upstream SSE disconnection crashes in strict state-machine clients like **ZCode Subagents** and **OpenAI Codex**.

---

## 🌟 Background & Problem

When using tools like ZCode or OpenAI Codex with proxy gateways such as [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI), long-running tasks (especially deep code-reviews or heavy reasoning runs) frequently suffer from the following fatal error:

```text
upstream stream closed before a terminal event (last event: response.output_item.done)
```

### Why does this happen?
1. In the OpenAI Responses API protocol, a stream turn must terminate with an explicit **terminal event** (`response.completed`, `response.failed`, or `response.incomplete`).
2. When the model finishes generating substantive output, it emits `finish_reason: "stop"` (mapped to `response.output_item.done`).
3. However, many upstream providers or gateways defer the emission of `response.completed` until an explicit `data: [DONE]` marker arrives from upstream.
4. If the upstream provider abruptly terminates the TCP connection (clean EOF, network jitter, or proxy idle reaping) after the last content chunk without delivering `[DONE]`, the gateway closes the downstream stream.
5. In an interactive main session, users rarely notice this because the text is already rendered and typing "continue" seamlessly resumes. **In autonomous Subagents, however, the turn is immediately flagged as an unhandled runtime failure (`status: failed`), aborting the entire subagent and forcing the orchestrator to restart the entire task from scratch.**

---

## 🛡️ How Nexus Stream Guard Fixes This

Nexus Stream Guard operates as a transparent sidecar proxy between your reverse-proxy / client and the upstream gateway:

```
[ Client: ZCode / Codex Subagents ]
                 │
                 ▼
[ Nginx Gateway / Reverse Proxy ]
   │
   ├── (Path = /v1/responses) ──► [ Nexus Stream Guard (Port 18318) ]
   │                                           │ (Transparent SSE stream monitoring)
   │                                           ▼
   └── (Path ^~ /v1/ all others) ─► [ CLIProxyAPI:8317 ]
                                               │
                                               ▼
                                  [ Upstream LLM Providers ]
```

### Safety & Decision Matrix

| Disconnection Scenario | Last Event Seen | Safety Assessment | Stream Guard Action |
| :--- | :--- | :--- | :--- |
| **Scenario A: Output Closed** | `response.output_item.done` | **Safe**: Output items are 100% closed; only terminal event missing. | **Synthesizes `response.completed`** with compliant schema, sorted outputs, and monotonic `sequence_number`. |
| **Scenario A (Reasoning Only)** | `output_item.done` (only reasoning item, no substantive text/tool item) | **Truncated**: Substantive content missing. | **Synthesizes `response.failed`** (`stream_truncated_after_reasoning`). |
| **Scenario B: Tool Arguments Cutoff** | `function_call_arguments.delta` | **Critical**: Incomplete JSON arguments. | **Synthesizes `response.failed`** (`stream_truncated_tool_args`) to prevent execution of malformed tool calls. |
| **Scenario C: Text Mid-stream Cutoff** | `output_text.delta` / `output_item.added` | **High**: Text truncated midway. | **Synthesizes `response.failed`** (`stream_truncated_text`) to avoid silent data loss. |
| **Scenario D: Reasoning Cutoff** | `reasoning_summary_text.delta` | **High**: Thinking truncated. | **Synthesizes `response.failed`** (`stream_truncated_reasoning`). |
| **Scenario E: Empty Stream** | `response.created` / `in_progress` | **Premature**: Disconnected before output. | **Synthesizes `response.failed`** (`stream_empty_or_premature`). |
| **Scenario F: Normal Terminal Event** | `response.completed` / `incomplete` / `failed` | **Normal**. | **Transparent pass-through**; zero injection. |
| **Idle Timeout Override (>=600s)** | Any | **Timeout**: Long connection dead. | **Forces `response.failed`** (`idle_timeout`). |
| **Default Reject** | Any unmapped state | **Unknown**. | **Synthesizes `response.failed`** (`stream_closed_unclean_default_reject`). |

---

## ⚡ Zero-Impact Surgical Isolation

Nexus Stream Guard is designed to be **surgically isolated**:
- **Non-Responses Endpoints (`/v1/chat/completions`, `/v1/messages`, `/v1/models`, etc.)**: Completely bypass Stream Guard at the Nginx level via `location ^~ /v1/`. They are 100% direct to your backend gateway with zero latency overhead.
- **Raw Byte Pipe**: Even for requests hitting Stream Guard, any non-streaming call or non-event-stream response is piped using raw chunked bytes without JSON parsing or body decoding.
- **Header Normalization**: Strips Hop-by-Hop headers and decodes/re-encodes safely while enforcing `Accept-Encoding: identity` upstream to prevent downstream gunzip mismatches.

---

## 🚀 Quick Start

### Option 1: Docker / Docker Compose (Recommended)

1. Build and run using Docker:
```bash
docker build -t cliproxyapi-stream-guard:v1.0 .
docker run -d \
  --name cliproxyapi-stream-guard \
  --restart unless-stopped \
  -e UPSTREAM_BASE_URL="http://cli-proxy-api:8317" \
  cliproxyapi-stream-guard:v1.0
```

2. Docker Compose snippet:
```yaml
services:
  cliproxyapi-stream-guard:
    image: cliproxyapi-stream-guard:v1.0
    container_name: cliproxyapi-stream-guard
    restart: unless-stopped
    environment:
      - HOST=0.0.0.0
      - PORT=18318
      - UPSTREAM_BASE_URL=http://cli-proxy-api:8317
      - UPSTREAM_VERIFY=false
      - READ_TIMEOUT=600.0
      - LOG_LEVEL=INFO
    networks:
      - default
```

### Option 2: Run with Python

```bash
pip install -r requirements.txt
python -m uvicorn tools.nexus_stream_guard.guard:app --host 127.0.0.1 --port 18318
```

---

## 🌐 Nginx Gateway Configuration

Add surgical routing in your Nginx reverse proxy (e.g. `nginx.conf`):

```nginx
# Route ONLY /v1/responses through Stream Guard
location = /v1/responses {
    proxy_pass http://cliproxyapi-stream-guard:18318;
    proxy_http_version 1.1;
    proxy_buffering off;
    proxy_cache off;
    chunked_transfer_encoding on;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}

# 100% Direct Passthrough for all other endpoints (/v1/chat/completions, /v1/models, etc.)
location ^~ /v1/ {
    proxy_pass http://cli-proxy-api:8317;
    proxy_http_version 1.1;
    proxy_buffering off;
    proxy_cache off;
    chunked_transfer_encoding on;
}
```

---

## 🧪 Testing & Verification

The project includes an automated end-to-end test suite covering all 24 scenarios:

```bash
python -m unittest tools.nexus_stream_guard.test_guard -v
```

All 24 test cases pass, verifying:
- Accurate synthesis of `response.completed` with monotonic `sequence_number`.
- Defense against malformed tool arguments and mid-text cuts.
- Upstream HTTP 500 / 4xx error transparent passthrough.
- Concurrency isolation across multiple parallel streams.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE) - matching the license of CLIProxyAPI.
