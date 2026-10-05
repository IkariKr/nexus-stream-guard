FROM python:3.11-alpine

WORKDIR /app

RUN apk add --no-cache curl \
    && pip install --no-cache-dir fastapi "uvicorn[standard]" httpx

COPY . /app/tools/nexus_stream_guard

ENV PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=18318 \
    UPSTREAM_BASE_URL=http://cli-proxy-api:8317 \
    UPSTREAM_VERIFY=false \
    READ_TIMEOUT=600.0 \
    LOG_LEVEL=INFO

EXPOSE 18318

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://127.0.0.1:18318/health || exit 1

CMD ["python", "-m", "uvicorn", "tools.nexus_stream_guard.guard:app", "--host", "0.0.0.0", "--port", "18318"]
