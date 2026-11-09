# restaurant-stream-backend

Python FastAPI service for the [restaurant-stream pipeline](https://github.com/7Chethan007/restaurant-stream-infra#readme).
Bridges the browser and the local Kinesis streams (via Floci) — no
restaurant-matching logic lives here, that's the Flink job's job.

## What this repo owns

Two responsibilities, one service:

- **Producer endpoint** — `POST /query` receives the text typed in
  `restaurant-stream-ui`, puts it onto the Kinesis **input stream**
  (`restaurant-queries`).
- **SSE endpoint** — `GET /stream` polls the Kinesis **output stream**
  (`restaurant-results`) and pushes matching results back to the browser
  that asked.

## How it fits the pipeline

```
Browser --POST text--> FastAPI --put_record--> Kinesis input stream
                                                     |
                                              Flink job (restaurant-stream-flink)
                                                     |
Browser <--SSE push-- FastAPI <--get_records-- Kinesis output stream
```

## Running it locally

Requires `restaurant-stream-infra`'s stack running first (`docker compose up -d`
there, then `./scripts/create-streams.sh`) — this service joins the same
`restaurant-stream-net` Docker network to reach Floci at `floci:4566`.

```
docker compose up -d
```

## Status

Not yet built. Planned stack: FastAPI, `boto3` (Kinesis client pointed at
`AWS_ENDPOINT_URL=http://floci:4566`), `sse-starlette` for the SSE endpoint.
