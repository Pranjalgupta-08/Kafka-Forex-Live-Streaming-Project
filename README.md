# Kafka Forex Live Streaming Project

Real-time Forex streaming pipeline using:

- Kafka (Confluent Cloud)
- AlphaVantage API
- AWS S3
- Spark / Delta Lake
- Streamlit Dashboard

## Architecture

AlphaVantage → Kafka Producer → Kafka Consumer → S3 → Spark (Bronze/Silver/Gold) → Streamlit

## How to Run

1. Create `.env` with credentials
2. Install requirements
3. Run producer
4. Run consumer
5. Start Streamlit app

## Tech Stack

Python, Kafka, AWS S3, Spark, Streamlit
