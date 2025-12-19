import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, List, Union

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

# --- Konfigurasi Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aggregator")

# --- Konfigurasi Environment ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@storage:5432/logs_db")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379/0")

# --- Model Data (Bagian B: Event Model) ---
class LogEvent(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict

# --- Global State ---
db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[redis.Redis] = None
start_time = time.time()  # ### UPDATE BAGIAN B: Untuk Uptime ###

# Statistik in-memory
stats = {
    "received": 0,
    "processed_success": 0,
    "duplicates_dropped": 0
}

# --- Database Setup ---
async def init_db():
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(255) NOT NULL,
                event_id VARCHAR(255) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                source VARCHAR(255),
                payload JSONB,
                received_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT unique_event_id UNIQUE (topic, event_id)
            );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_topic ON events(topic);")

# --- Background Worker ---
async def start_consumer():
    logger.info("Consumer worker started...")
    while True:
        try:
            msg = await redis_client.blpop("event_queue", timeout=5)
            if msg:
                raw_data = msg[1]
                event_data = json.loads(raw_data)
                
                async with db_pool.acquire() as conn:
                    async with conn.transaction():
                        try:
                            ts_val = datetime.fromisoformat(event_data['timestamp']).replace(tzinfo=None)
                            
                            result = await conn.execute("""
                                INSERT INTO events (topic, event_id, timestamp, source, payload)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (topic, event_id) DO NOTHING
                            """, 
                            event_data['topic'], 
                            event_data['event_id'],
                            ts_val,
                            event_data['source'],
                            json.dumps(event_data['payload'])
                            )
                            
                            if result == "INSERT 0 1":
                                stats["processed_success"] += 1
                                logger.info(f"Processed: {event_data['event_id']}")
                            else:
                                stats["duplicates_dropped"] += 1
                                logger.warning(f"Duplicate dropped: {event_data['event_id']}")
                                
                        except Exception as e:
                            logger.error(f"DB Error processing event: {e}")
        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
            await asyncio.sleep(1)

# --- Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, redis_client
    logger.info("Connecting to resources...")

    for i in range(5):
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            redis_client = redis.from_url(BROKER_URL)
            await init_db()
            break
        except Exception as e:
            logger.warning(f"Waiting for dependencies... {e}")
            await asyncio.sleep(2)
            
    logger.info("Starting 5 concurrent consumers...")
    for i in range(5):
        asyncio.create_task(start_consumer())

    yield

    logger.info("Shutting down...")
    await db_pool.close()
    await redis_client.close()

app = FastAPI(lifespan=lifespan, title="Distributed Log Aggregator")

# --- Bagian B: Endpoint Implementation ---

@app.post("/publish")
async def publish_event(event: Union[LogEvent, List[LogEvent]]):
    """
    Menerima Single atau Batch Event.
    Validasi skema otomatis oleh Pydantic.
    """
    events_to_process = event if isinstance(event, list) else [event]
    
    try:
        async with redis_client.pipeline() as pipe:
            for e in events_to_process:
                pipe.rpush("event_queue", e.model_dump_json())
            await pipe.execute()
            
        stats["received"] += len(events_to_process)
        return {"status": "queued", "count": len(events_to_process)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events(topic: Optional[str] = None):
    """Mengambil daftar event unik yang telah diproses."""
    query = "SELECT topic, event_id, timestamp, source, payload FROM events"
    args = []
    
    if topic:
        query += " WHERE topic = $1"
        args.append(topic)
    
    query += " ORDER BY timestamp DESC LIMIT 50"
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        return [dict(row) for row in rows]

@app.get("/stats")
async def get_stats():
    """
    Statistik lengkap: received, unique, dropped, topics, uptime.
    """
    # Hitung Uptime
    uptime_seconds = int(time.time() - start_time)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    
    # Ambil daftar Topik unik dari DB
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT topic FROM events")
        topics_list = [r['topic'] for r in rows]

    return {
        "received": stats["received"],
        "unique_processed": stats["processed_success"],
        "duplicate_dropped": stats["duplicates_dropped"],
        "topics": topics_list,      # ### UPDATE BAGIAN B ###
        "uptime": uptime_str        # ### UPDATE BAGIAN B ###
    }