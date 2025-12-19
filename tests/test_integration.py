import pytest
import httpx
import uuid
import asyncio
from datetime import datetime
import random

# Konfigurasi Target (Localhost karena dijalankan dari dalam container)
BASE_URL = "http://localhost:8080"

# --- Helper Functions ---
def generate_event(topic="test.integration", event_id=None, invalid_field=False):
    payload = {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "source": "pytest-runner",
        "payload": {"status": "ok", "value": 123}
    }
    if invalid_field:
        del payload["topic"] 
    return payload

async def get_stats():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/stats")
        return resp.json()

# --- GROUP 1: SCHEMA & VALIDATION ---

@pytest.mark.asyncio
async def test_01_publish_valid_event():
    event = generate_event()
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=event)
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

@pytest.mark.asyncio
async def test_02_publish_invalid_schema_missing_field():
    event = generate_event(invalid_field=True)
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=event)
        assert resp.status_code == 422

@pytest.mark.asyncio
async def test_03_publish_invalid_types():
    event = generate_event()
    event["topic"] = 12345 
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=event)
        assert resp.status_code == 422

@pytest.mark.asyncio
async def test_04_publish_empty_payload():
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json={})
        assert resp.status_code == 422

# --- GROUP 2: DEDUPLICATION LOGIC ---

@pytest.mark.asyncio
async def test_05_deduplication_single_topic():
    stats_awal = await get_stats()
    dropped_awal = stats_awal["duplicate_dropped"]
    
    event_id = str(uuid.uuid4())
    event = generate_event(event_id=event_id)
    
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=event)
        await client.post(f"{BASE_URL}/publish", json=event)
    
    await asyncio.sleep(1)
    
    stats_akhir = await get_stats()
    # Pake >= karena mungkin ada aktivitas background lain
    assert stats_akhir["duplicate_dropped"] >= dropped_awal + 1

@pytest.mark.asyncio
async def test_06_different_topics_same_id():
    """T6: Event ID sama tapi Topic beda -> Diterima keduanya."""
    # FIX: Gunakan topik unik (UUID) agar bisa difilter di DB yg penuh
    unique_topic = f"topic.T6.{uuid.uuid4()}"
    uid = str(uuid.uuid4())
    
    e1 = generate_event(topic=unique_topic, event_id=uid)
    e2 = generate_event(topic=unique_topic + ".B", event_id=uid) 
    
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=e1)
        await client.post(f"{BASE_URL}/publish", json=e2)
        
    await asyncio.sleep(1)
    
    # FIX: Cari menggunakan parameter ?topic=...
    async with httpx.AsyncClient() as client:
        # Kita cari satu persatu karena topicnya beda
        resp1 = await client.get(f"{BASE_URL}/events", params={"topic": unique_topic})
        resp2 = await client.get(f"{BASE_URL}/events", params={"topic": unique_topic + ".B"})
        
        data1 = resp1.json()
        data2 = resp2.json()
        
        # Harusnya masing-masing ketemu 1, jadi total ID yang sama ada 2 di sistem
        assert len(data1) >= 1
        assert len(data2) >= 1

@pytest.mark.asyncio
async def test_07_batch_publish_mixed():
    uid = str(uuid.uuid4())
    batch = [
        generate_event(),
        generate_event(event_id=uid),
        generate_event(event_id=uid)
    ]
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{BASE_URL}/publish", json=batch)
        assert resp.status_code == 200
        assert resp.json()["count"] == 3

@pytest.mark.asyncio
async def test_08_persistence_check():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/events")
        assert resp.status_code == 200
        assert len(resp.json()) > 0

# --- GROUP 3: CONCURRENCY ---

@pytest.mark.asyncio
async def test_09_concurrency_race_condition():
    """T9: Race Condition Test."""
    # FIX: Gunakan topik unik agar pencarian akurat
    unique_topic = f"topic.race.{uuid.uuid4()}"
    uid = str(uuid.uuid4())
    event = generate_event(topic=unique_topic, event_id=uid)
    
    async with httpx.AsyncClient() as client:
        tasks = [client.post(f"{BASE_URL}/publish", json=event) for _ in range(10)]
        await asyncio.gather(*tasks)
        
    await asyncio.sleep(2)
    
    # FIX: Cek DB dengan filter TOPIC agar tidak return 0
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/events", params={"topic": unique_topic})
        all_events = resp.json()
        # Filter lagi by ID untuk memastikan
        matches = [e for e in all_events if e['event_id'] == uid]
        assert len(matches) == 1, f"Race condition failed! Found {len(matches)}"

@pytest.mark.asyncio
async def test_10_high_concurrency_batch():
    events = [generate_event() for _ in range(50)]
    async with httpx.AsyncClient() as client:
        tasks = [client.post(f"{BASE_URL}/publish", json=e) for e in events]
        start = datetime.now()
        responses = await asyncio.gather(*tasks)
        end = datetime.now()
        
    assert all(r.status_code == 200 for r in responses)
    duration = (end - start).total_seconds()
    # print(f"\nBatch 50 req duration: {duration}s")
    assert duration < 5.0 

# --- GROUP 4: STATS & CONSISTENCY ---

@pytest.mark.asyncio
async def test_11_stats_structure():
    stats = await get_stats()
    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
    assert "topics" in stats
    assert "uptime" in stats

@pytest.mark.asyncio
async def test_12_stats_logic_consistency():
    stats = await get_stats()
    processed = stats["unique_processed"] + stats["duplicate_dropped"]
    assert stats["received"] >= processed

@pytest.mark.asyncio
async def test_13_filter_by_topic():
    """T13: Test endpoint GET /events?topic=..."""
    topic_name = f"test.filter.{uuid.uuid4()}" 
    
    # FIX: Syntax Error diperbaiki (menggunakan client context)
    async with httpx.AsyncClient() as client:
        await client.post(f"{BASE_URL}/publish", json=generate_event(topic=topic_name))
    
    await asyncio.sleep(1)
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/events", params={"topic": topic_name})
        data = resp.json()
        assert len(data) > 0
        assert all(e["topic"] == topic_name for e in data)

@pytest.mark.asyncio
async def test_14_uptime_format():
    stats = await get_stats()
    uptime = stats["uptime"]
    assert isinstance(uptime, str)
    assert ":" in uptime

# --- GROUP 5: SYSTEM HEALTH ---

@pytest.mark.asyncio
async def test_15_method_not_allowed():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/publish")
        assert resp.status_code == 405

@pytest.mark.asyncio
async def test_16_database_index_speed():
    start = datetime.now()
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/events")
        assert resp.status_code == 200
    duration = (datetime.now() - start).total_seconds()
    assert duration < 0.5