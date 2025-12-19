# Distributed Log Aggregator System

Repository ini berisi implementasi **Sistem Agregasi Log Terdistribusi** untuk pemenuhan Tugas Akhir Semester (UAS) mata kuliah Sistem Terdistribusi.

Sistem ini dirancang untuk menangani pengiriman event log dengan volume tinggi, menjamin *Idempotency* (mencegah duplikasi data), dan menjaga konsistensi data menggunakan arsitektur *Event-Driven*.

## 🏗️ Arsitektur Sistem

Sistem terdiri dari 4 layanan utama yang dijalankan menggunakan Docker Compose:

1.  **Publisher (Python):** Mensimulasikan klien yang mengirimkan event log terus-menerus. Memiliki fitur *Chaos Engineering* (sengaja mengirim duplikat 30% dari waktu) untuk menguji ketahanan sistem.
2.  **Aggregator (Python/FastAPI):** API Server yang bertindak sebagai gerbang masuk (*Gateway*). Menerima HTTP POST, melakukan validasi skema, dan meneruskan tugas ke antrean (*Queue*).
3.  **Broker (Redis):** Menyimpan antrean pesan sementara untuk memisahkan proses penerimaan (*ingestion*) dan pemrosesan (*processing*) agar *Non-blocking*.
4.  **Storage (PostgreSQL):** Penyimpanan persisten yang menerapkan *Unique Constraint* pada level database untuk menjamin deduplikasi yang atomik.

## 🚀 Cara Menjalankan (Run Instructions)

Pastikan **Docker Desktop** sudah terinstall dan berjalan.

1.  **Clone Repository (atau download):**
    ```bash
    git clone [https://github.com/USERNAME/uas-sistem-terdistribusi-2025.git](https://github.com/USERNAME/uas-sistem-terdistribusi-2025.git)
    cd uas-sistem-terdistribusi-2025
    ```

2.  **Jalankan Aplikasi:**
    ```bash
    docker compose up --build
    ```

3.  **Akses Aplikasi:**
    * **API Agregasi:** `http://127.0.0.1:8080`
    * **Monitoring Statistik:** `http://127.0.0.1:8080/stats`
    * **Lihat Data Event:** `http://127.0.0.1:8080/events`

## 🧪 Cara Menjalankan Testing

Proyek ini dilengkapi dengan 16 *Integration Tests* untuk memvalidasi fitur deduplikasi, konkurensi, dan validasi skema.

Karena aplikasi berjalan di dalam Docker, cara paling mudah menjalankan test adalah menumpang pada container `aggregator`:

1.  Pastikan aplikasi sedang berjalan (`docker compose up`).
2.  Buka terminal baru, lalu jalankan perintah berikut untuk menginstall *tools testing* dan menjalankan skenario:

    ```bash
    # 1. Install dependencies testing ke dalam container
    docker exec uas_aggregator pip install pytest pytest-asyncio httpx pydantic

    # 2. Copy folder tests ke dalam container
    docker cp tests uas_aggregator:/app/tests

    # 3. Jalankan Pytest
    docker exec uas_aggregator pytest -v /app/tests/test_integration.py
    ```

## 📡 API Endpoints

| Method | Endpoint | Deskripsi |
| :--- | :--- | :--- |
| `POST` | `/publish` | Menerima event log (Single/Batch JSON). |
| `GET` | `/stats` | Melihat statistik throughput, uptime, dan deduplikasi. |
| `GET` | `/events` | Melihat daftar event yang tersimpan (mendukung filter `?topic=...`). |

## ⚙️ Asumsi & Keputusan Desain

1.  **Deduplikasi di Database:** Kami memilih menggunakan *Database Unique Constraint* `(topic, event_id)` daripada Redis Cache untuk deduplikasi. Alasannya adalah untuk menjamin persistensi data yang kuat (*Strong Consistency*) bahkan jika container di-restart.
2.  **Ordering:** Sistem menggunakan *Source Timestamp* untuk pengurutan log secara logikal saat pembacaan (`ORDER BY timestamp DESC`).
3.  **Docker Network:** Semua komunikasi antar-service (Redis, Postgres, Python) terjadi di dalam jaringan internal `internal_network` yang terisolasi. Hanya port `8080` yang dibuka ke host.

## 📹 Video Demo & Laporan

* **Laporan Lengkap (PDF):** [Lihat file report.pdf](./report.pdf) *(Akan diupdate)*
* **Video Demo:** [Link YouTube] *(Akan diupdate)*