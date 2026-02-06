import json
import time
import requests
from confluent_kafka import Producer
import config

# ===========================
# Kafka Producer Config
# ===========================
producer_conf = {
    "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": config.KAFKA_API_KEY,
    "sasl.password": config.KAFKA_API_SECRET,
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Delivered | topic={msg.topic()} | "
            f"partition={msg.partition()} | offset={msg.offset()}"
        )

# last_sent_timestamp = None

# print("🚀 Starting Forex streaming producer...")

# try:
#     while True:
#         params = {
#             "function": config.ALPHA_VANTAGE_FUNCTION,
#             "from_currency": config.ALPHA_VANTAGE_FROM_CURRENCY,
#             "to_currency": config.ALPHA_VANTAGE_TO_CURRENCY,
#             "apikey": config.ALPHA_VANTAGE_API_KEY,
#         }

#         response = requests.get(
#             config.ALPHA_VANTAGE_URL,
#             params=params,
#             timeout=30,
#         )
#         response.raise_for_status()

#         data = response.json()
#         forex = data.get("Realtime Currency Exchange Rate")

#         if not forex:
#             print("⚠️ No data received")
#             time.sleep(config.POLL_INTERVAL_SECONDS)
#             continue

#         current_ts = forex["6. Last Refreshed"]

#         if current_ts == last_sent_timestamp:
#             print("⏳ No new update")
#         else:
#             message = {
#                 "from_currency": forex["1. From_Currency Code"],
#                 "to_currency": forex["3. To_Currency Code"],
#                 "exchange_rate": forex["5. Exchange Rate"],
#                 "last_refreshed": current_ts,
#                 "time_zone": forex["7. Time Zone"],
#             }

#             producer.produce(
#                 topic=config.KAFKA_TOPIC,
#                 key=f"{message['from_currency']}_{message['to_currency']}",
#                 value=json.dumps(message),
#                 callback=delivery_report,
#             )

#             producer.poll(0)
#             last_sent_timestamp = current_ts
#             print("📤 Published new rate")

#         time.sleep(config.POLL_INTERVAL_SECONDS)

# except KeyboardInterrupt:
#     print("\n🛑 Producer stopped")

# finally:
#     producer.flush()
#     print("✅ Producer shutdown complete")


last_sent_timestamp = {}

print("🚀 Starting Forex streaming producer...")

try:
    while True:
        for from_cur, to_cur in config.FOREX_PAIRS:

            params = {
                "function": config.ALPHA_VANTAGE_FUNCTION,
                "from_currency": from_cur,
                "to_currency": to_cur,
                "apikey": config.ALPHA_VANTAGE_API_KEY,
            }

            response = requests.get(
                config.ALPHA_VANTAGE_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            data = response.json()
            forex = data.get("Realtime Currency Exchange Rate")

            if not forex:
                print(f"⚠️ No data for {from_cur}_{to_cur}")
                continue

            current_ts = forex["6. Last Refreshed"]
            pair_key = f"{from_cur}_{to_cur}"

            if last_sent_timestamp.get(pair_key) == current_ts:
                print(f"⏳ No new update for {pair_key}")
                continue


            message = {
                "from_currency": from_cur,
                "to_currency": to_cur,
                "exchange_rate": forex["5. Exchange Rate"],
                "last_refreshed": current_ts,
                "time_zone": forex["7. Time Zone"],
            }

            producer.produce(
                topic=config.KAFKA_TOPIC,
                key=pair_key,
                value=json.dumps(message),
                callback=delivery_report,
            )

            producer.poll(0)
            last_sent_timestamp[pair_key] = current_ts

            print(f"📤 Published {pair_key}")

            # AlphaVantage rate limit safety
            time.sleep(5)

        time.sleep(config.POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped")

finally:
    producer.flush()
    print("✅ Producer shutdown complete")
