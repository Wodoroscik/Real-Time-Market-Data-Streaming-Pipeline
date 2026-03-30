# Real-Time Market Data Streaming Pipeline

An end-to-end data engineering PoC demonstrating real-time stream processing of financial data. The pipeline simulates high-frequency market ticks, buffers them using a distributed message broker, aggregates the data over time windows, and visualizes the results on a live dashboard.

## Architecture

1. **Data Generation (`producer.py`)**: A Python script fetches baseline daily close prices for selected tickers (AAPL, MSFT, GOOGL) via the `yfinance` API. It then simulates real-time market volatility by applying continuous random noise and produces JSON payloads.
2. **Message Broker (`docker-compose.yml`)**: Apache Kafka running in a Docker container. Configured to use KRaft mode (ZooKeeper-less architecture) for metadata management.
3. **Stream Processing (`consumer.py`)**: PySpark application that subscribes to the Kafka topic. It deserializes the JSON payloads, casts Unix timestamps, and applies a **30-second tumbling window** with a **10-second watermark** to handle late-arriving data. The micro-batches are written to a local SQLite database.
4. **Presentation Layer (`dashboard.py`)**: A Streamlit application that continuously polls the SQLite database, implements upsert-like deduplication logic for the tumbling windows, and renders real-time line charts.

## Key Engineering Concepts Demonstrated

* **Decoupled Architecture**: Separation of data ingestion (Producer) and processing (Consumer) using Apache Kafka as a persistent buffer.
* **Windowing & Late Data Handling**: Implementation of tumbling windows and watermarking in PySpark to maintain state and handle out-of-order network packets.
* **Infrastructure as Code**: Containerized Kafka deployment using Docker Compose.

## Prerequisites

* **Docker & Docker Compose** (for the Kafka broker)
* **Python 3.10+**
* **Java 17 JRE** (Required for the PySpark JVM gateway)

## Quick Start

**1. Start the Kafka Broker**
Deploy the Kafka container in the background:
```bash
sudo docker compose up -d
```
**2. Setup Python Environment**
Initialize and activate a virtual environment, then install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. Run the Pipeline
You will need three separate terminal windows (ensure the venv is activated in each).

Terminal 1 (Start Data Ingestion):
```bash
python producer.py
```
Terminal 2 (Start Stream Processing):
```bash
python consumer.py
```
Terminal 3 (The dashboard will be available at http://localhost:8501.):
```bash
streamlit run dashboard.py
```
4. Teardown
Stop the Python scripts using Ctrl+C. To stop and remove the Kafka container and its associated data volumes:
```bash
sudo docker compose down -v
```
Production Considerations (Limitations of this PoC)
This architecture was designed for local demonstration and development. In a production environment, the following architectural changes would be necessary:
- Storage Sink: SQLite is a file-based relational database unsuitable for high-throughput concurrent writes. It should be replaced with a dedicated Time-Series Database such as InfluxDB or Prometheus.
- Visualization: Streamlit does not natively support WebSockets for live streaming and relies on active polling, which creates I/O bottlenecks. The presentation layer should be migrated to Grafana, which natively integrates with TSDBs and supports push-based live updates.
- Cluster Deployment: The PySpark application should be deployed on a real cluster (e.g., Databricks, Amazon EMR, or standalone Spark cluster via Kubernetes) instead of running in local mode.
