import asyncio
import httpx
import os
import random
import uuid
import logging
from datetime import datetime, timezone
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("publisher")

TARGET_URL = os.getenv("TARGET_URL", "http://localhost:8080/publish")
TICK_INTERVAL = float(os.getenv("TICK_INTERVAL_MS", 500)) / 1000.0  

# Buffer untuk menyimpan riwayat event yang sudah dikirim (untuk keperluan duplikasi)
sent_events_history = deque(maxlen=50)

TOPICS = ["system.logs", "payment.gateway", "user.auth", "order.processing"]
SOURCES = ["service-a", "service-b", "mobile-app", "web-frontend"]

def generate_new_event():
    """Membuat event baru yang unik."""
    return {
        "topic": random.choice(TOPICS),
        "event_id": str(uuid.uuid4()), 
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": random.choice(SOURCES),
        "payload": {
            "cpu_usage": random.randint(10, 90),
            "memory": random.randint(100, 1024),
            "status": "active"
        }
    }

async def main():
    logger.info(f"Publisher started. Target: {TARGET_URL}")
    logger.info("Simulating traffic with random duplicates...")

    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                is_duplicate = False
                event_data = None

                if sent_events_history and random.random() < 0.3:
                    # AMBIL EVENT LAMA (DUPLIKASI)
                    event_data = random.choice(sent_events_history)
                    is_duplicate = True
                else:
                    # BUAT EVENT BARU
                    event_data = generate_new_event()
                    sent_events_history.append(event_data)

                # Kirim Request POST
                response = await client.post(TARGET_URL, json=event_data)

                # Logging Visual
                topic_tag = f"[{event_data['topic']}]"
                id_short = event_data['event_id'][:8]
                
                if response.status_code == 200:
                    if is_duplicate:
                        logger.warning(f"♻️  SENT DUPLICATE: {id_short} | Status: {response.status_code}")
                    else:
                        logger.info(f"✅ SENT NEW      : {id_short} | Status: {response.status_code}")
                else:
                    logger.error(f"❌ FAILED: {response.status_code} - {response.text}")

            except Exception as e:
                logger.error(f"Connection Error: {e}")

            # Tunggu sebelum tick berikutnya
            await asyncio.sleep(TICK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())