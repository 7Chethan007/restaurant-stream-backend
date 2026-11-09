import asyncio
import json
import os
import threading
import time
import uuid

import boto3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

KINESIS_ENDPOINT = os.environ.get("KINESIS_ENDPOINT", "http://floci:4566")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
INPUT_STREAM = "restaurant-queries"
OUTPUT_STREAM = "restaurant-results"

kinesis = boto3.client(
    "kinesis",
    endpoint_url=KINESIS_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# id -> result, filled in by the background poller below. Fine as a plain
# dict for a single-process local demo; would need a real store for more
# than one backend instance.
results: dict[str, dict] = {}


class QueryRequest(BaseModel):
    text: str


@app.post("/query")
def submit_query(req: QueryRequest):
    query_id = str(uuid.uuid4())
    payload = json.dumps({"id": query_id, "text": req.text})
    kinesis.put_record(
        StreamName=INPUT_STREAM,
        Data=payload.encode("utf-8"),
        PartitionKey=query_id,
    )
    return {"id": query_id}


@app.get("/stream/{query_id}")
async def stream_result(query_id: str):
    async def event_generator():
        waited = 0.0
        while waited < 30:
            if query_id in results:
                yield {"event": "result", "data": json.dumps(results[query_id])}
                return
            await asyncio.sleep(0.5)
            waited += 0.5
        yield {"event": "timeout", "data": "no match found in time"}

    return EventSourceResponse(event_generator())


def poll_results():
    # The output stream may not exist yet right at backend startup (Floci
    # just came up too) -- retry instead of crashing the poller thread.
    shard_id = None
    while shard_id is None:
        try:
            shard_id = kinesis.describe_stream(StreamName=OUTPUT_STREAM)[
                "StreamDescription"
            ]["Shards"][0]["ShardId"]
        except Exception:
            time.sleep(1)

    iterator = kinesis.get_shard_iterator(
        StreamName=OUTPUT_STREAM, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    while True:
        response = kinesis.get_records(ShardIterator=iterator, Limit=25)
        for record in response["Records"]:
            data = json.loads(record["Data"])
            results[data["id"]] = data
        iterator = response.get("NextShardIterator")
        if iterator is None:
            break
        time.sleep(0.5)


@app.on_event("startup")
def start_poller():
    thread = threading.Thread(target=poll_results, daemon=True)
    thread.start()
