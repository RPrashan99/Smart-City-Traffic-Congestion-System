# Smart City Traffic Congestion System

A real-time traffic monitoring and analytics platform built on a modern big data stack. The system simulates traffic sensor data, streams it through Kafka into Spark for live congestion detection, orchestrates daily batch analytics with Airflow, and generates PDF reports.

## Architecture

```
Traffic Sensors (Simulated)
        |
        v
  [Producer] ──── Kafka (traffic_raw) ────► [Spark Streaming]
                                                  |        |
                                            HDFS (agg,   Kafka
                                             alerts)   (critical-alerts)
                                                  |
                                         [Airflow DAG] (daily @ midnight)
                                                  |
                                       [Daily Peak Hour Batch Job]
                                                  |
                                         Local CSV / HDFS
                                                  |
                                        [Report Generator]
                                                  |
                                          PDF Analytics Report
```

## Components

| Component | Path | Description |
|-----------|------|-------------|
| Traffic Producer | `producer/traffic_producer.py` | Simulates sensor events from 4 junctions and publishes to Kafka |
| Spark Streaming | `spark_app/traffic_streaming.py` | Real-time 5-minute window aggregations and congestion alerts |
| Daily Batch Job | `spark_app/daily_peak_hour.py` | Identifies peak traffic hours and generates summary statistics |
| Airflow DAG | `airflow/dags/traffic_daily_peak_hour.py` | Schedules the daily batch Spark job |
| Report Generator | `project_reports/report_generator.py` | Produces a PDF report with charts and tables |

## Tech Stack

- **Streaming:** Apache Kafka, Apache Spark Streaming 3.5.x
- **Batch Processing:** Apache Spark, Apache Airflow 2.9.0
- **Storage:** HDFS (Parquet/CSV)
- **Reporting:** Pandas, Matplotlib, ReportLab
- **Containerization:** Docker
- **Language:** Python 3.x

## Data Flow

1. **Producer** generates sensor events (sensor ID, timestamp, vehicle count, avg speed) for junctions J1–J4 every second, with a 5% chance of a critical congestion event (speed < 10 km/h), and publishes them to the Kafka topic `traffic_raw`.

2. **Spark Streaming** consumes `traffic_raw`, computes 5-minute rolling window aggregations (total vehicles, avg speed, congestion index), and writes:
   - Aggregated data → `hdfs://namenode:8020/data/traffic/agg_5min` (Parquet)
   - Congestion alerts → `hdfs://namenode:8020/data/traffic/alerts` (Parquet) + Kafka `critical-alerts`

3. **Airflow DAG** triggers the daily batch job at midnight via a `SparkSubmitOperator`.

4. **Daily Batch Job** reads the previous day's aggregated HDFS data, identifies peak hours per junction, and writes:
   - Peak hour report → `hdfs://namenode:8020/reports/` (CSV)
   - Daily summary → local CSV files for report generation

5. **Report Generator** reads the local CSVs, produces bar/line charts, and compiles a `traffic_analytics_report.pdf`.

## Project Structure

```
Smart-City-Traffic-Congestion-System/
├── producer/
│   └── traffic_producer.py
├── spark_app/
│   ├── dockerfile
│   ├── requirements.txt
│   ├── traffic_streaming.py
│   └── daily_peak_hour.py
├── airflow/
│   ├── dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── traffic_daily_peak_hour.py
└── project_reports/
    └── report_generator.py
```

## Prerequisites

- Docker
- A running Kafka broker (default: `localhost:29092`)
- A running Spark cluster (default master: `spark://spark-master:7077`)
- A running HDFS NameNode (default: `hdfs://namenode:8020`)
- Apache Airflow 2.9.0 (or use the provided Dockerfile)

## Getting Started

### 1. Build Docker Images

```bash
# Spark application image
docker build -t spark-traffic ./spark_app

# Airflow image
docker build -t airflow-traffic ./airflow
```

### 2. Start the Traffic Producer

```bash
cd producer
pip install kafka-python
python traffic_producer.py
```

### 3. Start Spark Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_app/traffic_streaming.py
```

### 4. Start Airflow and Trigger the DAG

```bash
airflow db init
airflow webserver &
airflow scheduler &
# Enable the traffic_daily_peak_hour DAG in the Airflow UI
```

### 5. Generate the Report

```bash
cd project_reports
pip install pandas matplotlib reportlab
python report_generator.py
```

The PDF report is saved as `traffic_analytics_report.pdf`.

## Key Configuration

| Setting | Default | Location |
|---------|---------|----------|
| Kafka broker | `localhost:29092` | `producer/traffic_producer.py` |
| Kafka topic | `traffic_raw` | Producer & Streaming |
| Spark master | `spark://spark-master:7077` | Airflow DAG |
| HDFS NameNode | `hdfs://namenode:8020` | Streaming & Batch jobs |
| Streaming window | 5 minutes | `traffic_streaming.py` |
| Congestion threshold | speed < 10 km/h | Streaming & Producer |
| Batch schedule | Daily @ midnight | Airflow DAG |

## Output

- **Real-time console** — live aggregation output from Spark Streaming
- **Kafka `critical-alerts`** — downstream-consumable congestion events
- **HDFS Parquet files** — aggregated and alert data for historical analysis
- **CSV reports** — daily peak hour and traffic volume summaries
- **`traffic_analytics_report.pdf`** — traffic volume bar chart, avg speed line chart, peak hour table
