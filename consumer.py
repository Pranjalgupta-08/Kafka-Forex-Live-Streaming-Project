import json
from datetime import datetime
from confluent_kafka import Consumer
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import config
import io
import time


# ===========================
# Kafka Consumer Config
# ===========================
consumer_conf = {
    "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": config.KAFKA_API_KEY,
    "sasl.password": config.KAFKA_API_SECRET,
    "group.id": config.KAFKA_CONSUMER_GROUP,
    "auto.offset.reset": config.AUTO_OFFSET_RESET,  # earliest
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)
consumer.subscribe([config.KAFKA_TOPIC])

# ===========================
# S3 Client
# ===========================
s3 = boto3.client(
    "s3",
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION,
)

buffer = []

print("🚀 Kafka consumer started")
print(f"📌 Topic: {config.KAFKA_TOPIC}")
print(f"📌 Consumer group: {config.KAFKA_CONSUMER_GROUP}")
print(f"📌 Auto offset reset: {config.AUTO_OFFSET_RESET}")
print("📥 Waiting for messages...\n")

def build_s3_key(topic: str):
    now = datetime.utcnow()
    return (
        f"{config.S3_BASE_PREFIX}/{topic}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"forex_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    )

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("⏳ No message yet (poll timeout)")
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # ---------------------------
        # LOG OFFSET + PARTITION
        # ---------------------------
        print(
            f"🔥 Consumed | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()}"
        )

        record = json.loads(msg.value().decode("utf-8"))
        buffer.append(record)

        print(f"📥 Buffered records: {len(buffer)}")

        # ---------------------------
        # Flush to Parquet
        # ---------------------------
        if len(buffer) >= config.PARQUET_BATCH_SIZE:
            print("🧊 Flushing buffer to Parquet...")

            df = pd.DataFrame(buffer)
            table = pa.Table.from_pandas(df)

            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)

            s3_key = build_s3_key(msg.topic())

            s3.put_object(
                Bucket=config.S3_BUCKET_NAME,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
            )

            print(
                f"✅ Parquet written | records={len(buffer)} | "
                f"s3://{config.S3_BUCKET_NAME}/{s3_key}\n"
            )

            buffer.clear()

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user")

finally:
    consumer.close()
    print("✅ Consumer shutdown complete")
