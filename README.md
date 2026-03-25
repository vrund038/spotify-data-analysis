# 🎧 Spotify Data Engineering Pipeline (End‑to‑End)

This project demonstrates a **Modern Data Engineering Pipeline** using **Airflow, Kafka, MinIO, PostgreSQL, and dbt Core** to build a **Bronze → Silver → Gold** architecture for Spotify event data.

---

# 🚀 Architecture Overview

```
Producer → Kafka → Kafdrop → Airflow → MinIO (Bronze)
                                      ↓
                                   PostgreSQL
                                      ↓
                               dbt (Silver & Gold)
                                      ↓
                                  Analytics / BI
```

---

# 🧰 Tech Stack

| Tool           | Purpose                      |
| -------------- | ---------------------------- |
| Apache Kafka   | Data Streaming               |
| Kafdrop        | Kafka UI Monitoring          |
| Apache Airflow | Workflow Orchestration       |
| MinIO          | Data Lake (Bronze Layer)     |
| PostgreSQL     | Data Warehouse               |
| dbt Core       | Data Transformation          |
| Python         | Data Generation & Processing |
| Docker         | Containerization             |

---

# 📂 Project Structure

```
spotify-data-pipeline
│
├── airflow/
│   └── dags/
│
├── producer/
│   └── generate_spotify_data.py
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   │
│   └── sources.yml
│
├── docker/
│   └── docker-compose.yml
│
├── requirements.txt
└── README.md
```

---

# 🔄 Data Pipeline Flow

## 1️⃣ Producer Layer

* Python script generates dummy Spotify streaming data
* Data pushed to Kafka topic

Example Fields:

* event_id
* user_id
* song_id
* artist_name
* song_name
* event_type
* device_type
* country
* timestamp

---

## 2️⃣ Kafka + Kafdrop

Kafka stores streaming events.

Kafdrop used for:

* Monitoring topics
* Viewing messages
* Debugging pipeline

---

## 3️⃣ Airflow Orchestration

Airflow DAG performs:

* Consume Kafka events
* Store raw data in MinIO (Bronze)
* Load data into PostgreSQL

---

## 4️⃣ Bronze Layer (MinIO)

Raw data stored in:

```
minio/spotify/bronze/date=YYYY-MM-DD/
```

Characteristics:

* Raw data
* No transformations
* Historical storage

---

## 5️⃣ PostgreSQL Data Warehouse

Data loaded into:

```
bronze.spotify_events
```

Used for further transformation using dbt.

---

# 🔧 dbt Transformation

## Bronze Model

Raw data from PostgreSQL

```
bronze.spotify_events
```

---

## Silver Layer

Cleaning & Validation

* Remove null values
* Standardize timestamps
* Data quality checks

Example Model:

```
models/silver/spotify_silver.sql
```

---

## Gold Layer

Business Metrics

Examples:

* Top Songs
* Top Artists
* Country-wise listening
* Device usage analytics

---

# ▶️ How To Run Project

## 1️⃣ Start Docker Services

```
docker-compose up -d
```

---

## 2️⃣ Start Airflow

Open:

```
http://localhost:8080
```

---

## 3️⃣ Start Kafdrop

Open:

```
http://localhost:9000
```

---

## 4️⃣ Run dbt

```
dbt run
```

---

## 5️⃣ Run Tests

```
dbt test
```

---

# 📊 Sample Analytics

This project enables:

* Top 10 songs
* Most active users
* Listening trends
* Device usage
* Country analysis

---

# 🔐 Environment Variables

Example `.env`

```
MINIO_ENDPOINT=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=

POSTGRES_HOST=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

KAFKA_BROKER=
```

---

# 📸 Architecture Diagram

```
Kafka → Airflow → MinIO → PostgreSQL → dbt → Analytics
```

---

# 💡 Key Features

✅ End‑to‑End Pipeline
✅ Real‑Time Streaming
✅ Data Lake + Warehouse
✅ dbt Transformations
✅ Scalable Architecture

---

# 🧠 Learning Outcomes

* Data Engineering Pipeline Design
* Kafka Streaming
* Airflow Orchestration
* Data Lake Architecture
* dbt Transformations
* PostgreSQL Data Warehouse

---

# 👨‍💻 Author

Vrund Patel
Data Engineering Project



Feel free to connect for collaboration
