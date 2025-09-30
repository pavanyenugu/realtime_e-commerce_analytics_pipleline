# Real-Time E-Commerce Analytics Pipeline

## Overview
This project simulates a real-time analytics pipeline using **Kafka, Spark Streaming, Airflow, Redshift, Prometheus, and Grafana**.

### Components
- **Kafka** → Streams e-commerce order events
- **Spark Streaming** → Processes and aggregates orders
- **Redshift** → Stores processed data
- **Airflow** → Orchestrates the workflow
- **Prometheus + Grafana** → Monitoring and dashboards

### Steps to Run
1. Start Kafka and create a topic `orders`.
2. Run `kafka_producer/producer.py` to generate events.
3. Run Spark job:  
   ```bash
   spark-submit spark_streaming/spark_stream.py

