# restaurant-stream-backend

Python FastAPI service for the [restaurant-stream pipeline](https://github.com/7Chethan007/restaurant-stream-infra#readme).
Bridges the browser and GCP Pub/Sub — no restaurant-matching logic lives
here, that's the Flink job's job.

## What this repo owns

Two responsibilities, one service:

- **Producer endpoint** — `POST /query` receives the text typed in
  `restaurant-stream-ui`, publishes it to the Pub/Sub **input topic**
  (`restaurant-queries`).
- **SSE endpoint** — `GET /stream` subscribes to the Pub/Sub **output topic**
  (`restaurant-results`) and pushes matching results back to the browser
  that asked.

## How it fits the pipeline

```
Browser --POST text--> FastAPI --publish--> Pub/Sub input topic
                                                     |
                                              Flink job (restaurant-stream-flink)
                                                     |
Browser <--SSE push-- FastAPI <--subscribe-- Pub/Sub output topic
```

## Deployment

Runs on the GCE VM provisioned by `restaurant-stream-infra`, via Docker
Compose alongside the Flink job. This repo's own `.github/workflows` builds
the image and SSHes to the VM to redeploy on push to `main`.

## Status

Not yet built. Planned stack: FastAPI, `google-cloud-pubsub` client,
`sse-starlette` for the SSE endpoint.
