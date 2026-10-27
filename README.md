# 🛒 Enterprise Retail Medallion Lakehouse

A real-time retail analytics pipeline built on the **Medallion Architecture** (Bronze → Silver → Gold). Simulated point-of-sale transactions are streamed through **Apache Kafka**, landed as raw **Parquet** files, cleaned and refined, aggregated into business-ready metrics, and served through a **Streamlit** dashboard.

Everything runs locally: Kafka in Docker, the lakehouse layers as Parquet files on disk.

---

## Architecture

```
┌────────────┐    ┌─────────────────────┐    ┌────────────────────────┐
│ producer.py│───▶│ Kafka                │───▶│ consumer_bronze.py     │
│ (mock POS) │    │ topic:               │    │ batches of 10 → Parquet│
└────────────┘    │ retail-transactions  │    └───────────┬────────────┘
                  └─────────────────────┘                │
                                                          ▼
                                              ┌────────────────────────┐
                                              │ 🥉 BRONZE              │
                                              │ bronze_layer/          │
                                              │ (raw events)           │
                                              └───────────┬────────────┘
                                                          │ silver_transformation.py
                                                          ▼
                                              ┌────────────────────────┐
                                              │ 🥈 SILVER              │
                                              │ silver_layer/          │
                                              │ (cleaned, typed)       │
                                              └───────────┬────────────┘
                                                          │ gold_aggregation.py
                                                          ▼
                                              ┌────────────────────────┐
                                              │ 🥇 GOLD                │
                                              │ gold_layer/            │
                                              │ (business metrics)     │
                                              └───────────┬────────────┘
                                                          │
                                                          ▼
                                              ┌────────────────────────┐
                                              │ app.py (Streamlit)     │
                                              │ analytics dashboard    │
                                              └────────────────────────┘
```

### Layers

| Layer | Location | What happens |
|-------|----------|--------------|
| 🥉 **Bronze** | `bronze_layer/retail_transactions/` | Raw Kafka events written as-is to Parquet, one file per 10 transactions (`batch_<timestamp>.parquet`). |
| 🥈 **Silver** | `silver_layer/retail_transactions/` | Deduplicates on `transaction_id`, casts `quantity` / `unit_price` / `total_amount` to proper types, uppercases `store_id`, and adds a `silver_processed_at` audit column. |
| 🥇 **Gold** | `gold_layer/` | Aggregated tables ready for BI tools: `sales_by_store` and `top_products`. |


### Gold tables

- **`sales_by_store/metrics_by_store.parquet`**: total revenue, transaction count, and average basket size per store.
- **`top_products/metrics_top_products.parquet`**: total quantity sold and revenue per product, ranked by units sold.

---

## Project Structure

```
Enterprise-Retail-Medallion-Lakehouse/
├── docker-compose.yml          # Kafka (KRaft mode) + Kafka UI
├── producer.py                 # Generates mock retail transactions → Kafka
├── consumer.py                 # Simple console consumer for debugging
├── consumer_bronze.py          # Kafka → Bronze layer (Parquet)
├── silver_transformation.py    # Bronze → Silver (clean & standardize)
├── gold_aggregation.py         # Silver → Gold (business metrics)
├── app.py                      # Streamlit dashboard over the Gold layer
├── requirements.txt
└── .gitignore                  # Excludes bronze_layer/, silver_layer/, gold_layer/
```

---

## Tech Stack

- **Apache Kafka** (Confluent `cp-kafka:7.8.0`, KRaft mode, no ZooKeeper)
- **Kafka UI** (Provectus) for inspecting topics and messages
- **Python**: `kafka-python-ng`, `pandas`, `pyarrow` (Parquet I/O)
- **Streamlit** for the dashboard
- **Docker Compose** for local infrastructure

---

## Getting Started

### Prerequisites

- Python 3.9+
- Docker and Docker Compose

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd Enterprise-Retail-Medallion-Lakehouse

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
pip install pyarrow streamlit   # needed for Parquet I/O and the dashboard
```

> `requirements.txt` currently lists `kafka-python-ng`, `pandas`, and `requests`. `pyarrow` and `streamlit` are required by the scripts, so install them as shown above (or add them to `requirements.txt`).

### 2. Start Kafka

```bash
docker-compose up -d
```

| Service | URL / Port |
|---------|-----------|
| Kafka broker | `localhost:9092` |
| Kafka UI | http://localhost:9000 |

### 3. Stream data into the Bronze layer

Open two terminals (with the virtual environment activated).

**Terminal 1: start the producer** (emits one transaction every 2 seconds):

```bash
python producer.py
```

**Terminal 2: start the Bronze consumer** (writes a Parquet file every 10 transactions):

```bash
python consumer_bronze.py
```

Let it run until a few batch files appear in `bronze_layer/retail_transactions/`, then stop both with `Ctrl+C`.

> Optional: run `python consumer.py` instead to just print live transactions to the console without writing any files.

### 4. Build the Silver and Gold layers

```bash
python silver_transformation.py
python gold_aggregation.py
```

The Gold script prints both metric tables to the console and saves them under `gold_layer/`.

### 5. Launch the dashboard

```bash
streamlit run app.py
```

Then open http://localhost:8501.

---

## Dashboard

The Streamlit app reads the Gold layer and shows:

- **KPIs**: total enterprise revenue and total transactions processed
- **Revenue by store**: bar chart
- **Top-selling products**: bar chart of units sold
- **Detail tables**: store performance metrics and product sales breakdown

If the Gold Parquet files are not found (for example on Streamlit Community Cloud, where the local data layers are git-ignored), the app falls back to built-in **sample data** so the dashboard still renders.

---

## Event Schema

Each message on the `retail-transactions` topic is a JSON document:

```json
{
  "transaction_id": "TXN-482913",
  "store_id": "Store_NY_01",
  "item_id": "P1003",
  "item_name": "HD Monitor",
  "quantity": 2,
  "unit_price": 189.5,
  "total_amount": 379.0,
  "timestamp": "2026-10-27T06:44:12.123456"
}
```

**Mock master data**

- Stores: `Store_NY_01`, `Store_LA_02`, `Store_CHI_03`, `Store_TX_04`
- Products: Wireless Mouse, Mechanical Keyboard, HD Monitor, USB-C Hub, Ergonomic Chair

---

## Design Notes and Limitations

- The pipeline is **local and file-based**. Silver and Gold are produced by batch scripts you run manually, not by a scheduler.
- `silver_transformation.py` reprocesses every Bronze file on each run and overwrites the matching Silver file. Deduplication happens **within each batch file**, not across files.
- `gold_aggregation.py` recomputes the metrics from all Silver files on each run (a full refresh).
- Transaction IDs are random 6-digit numbers, so collisions are possible over long runs.
- Kafka is configured as a single node with replication factor 1, which suits development only.

## Ideas for Extension

- Orchestrate the Bronze → Silver → Gold steps with **Airflow** or Prefect
- Move transformations to **Spark** or **dbt** for scale
- Use Delta Lake or Iceberg for ACID tables and incremental processing
- Swap local storage for **S3 / ADLS** and a warehouse such as Snowflake or Databricks
- Add data quality checks (e.g. Great Expectations) between layers
- Connect the Gold layer to Power BI or Superset

