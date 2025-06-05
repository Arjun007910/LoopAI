import requests

BASE_URL = "http://127.0.0.1:8000"

def test_ingestion():
    data = {"ids": [1, 2, 3], "priority": "HIGH"}
    response = requests.post(f"{BASE_URL}/ingest", json=data)
    assert response.status_code == 200
    assert "ingestion_id" in response.json()

def test_status():
    ingestion_id = test_ingestion()["ingestion_id"]
    response = requests.get(f"{BASE_URL}/status/{ingestion_id}")
    assert response.status_code == 200
    assert "status" in response.json()

test_ingestion()
test_status()
