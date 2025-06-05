from fastapi import FastAPI
import uuid
import time
import threading

app = FastAPI()
data_store = {}

@app.post("/ingest")
def ingest(data: dict):
    ingestion_id = str(uuid.uuid4())
    data_store[ingestion_id] = {"status": "yet_to_start", "batches": []}
    ids = data["ids"]
    priority = data["priority"]
    batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
    threading.Thread(target=process_batches, args=(ingestion_id, batches)).start()
    return {"ingestion_id": ingestion_id}
def process_batches(ingestion_id, batches):
    for batch in batches:
        time.sleep(5) 
        batch_id = str(uuid.uuid4())
        data_store[ingestion_id]["batches"].append({"batch_id": batch_id, "ids": batch, "status": "completed"})
        statuses = [b["status"] for b in data_store[ingestion_id]["batches"]]
        if "triggered" in statuses:
            data_store[ingestion_id]["status"] = "triggered"
        elif all(s == "completed" for s in statuses):
            data_store[ingestion_id]["status"] = "completed"
@app.get("/status/{ingestion_id}")
def get_status(ingestion_id: str):
    return data_store.get(ingestion_id, {"error": "Not found"})