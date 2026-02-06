

from dotenv import load_dotenv
import os

# Load environment variables

load_dotenv()


# ===========================
# Streaming Config
# ===========================
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))


# ===========================
# Kafka Config
# ===========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Consumer-specific
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "forex-consumer-parquet-v1")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

# ===========================
# Alpha Vantage Forex Config
# ===========================
ALPHA_VANTAGE_URL = os.getenv("ALPHA_VANTAGE_URL")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
ALPHA_VANTAGE_FUNCTION = os.getenv("ALPHA_VANTAGE_FUNCTION")
# ALPHA_VANTAGE_FROM_CURRENCY = os.getenv("ALPHA_VANTAGE_FROM_CURRENCY")
# ALPHA_VANTAGE_TO_CURRENCY = os.getenv("ALPHA_VANTAGE_TO_CURRENCY")
# ===========================
# Forex Pairs
# ===========================
ALPHA_VANTAGE_PAIRS = os.getenv("ALPHA_VANTAGE_PAIRS", "USD:INR")

FOREX_PAIRS = [
    tuple(pair.split(":"))
    for pair in ALPHA_VANTAGE_PAIRS.split(",")
]


# ===========================
# Parquet Config
# ===========================
PARQUET_BATCH_SIZE = int(os.getenv("PARQUET_BATCH_SIZE", "10"))


# ===========================
# S3 Config
# ===========================
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "custom-consumer")

# ===========================
# Validation (Fail Fast)
# ===========================
required_vars = {
    "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
    "KAFKA_API_KEY": KAFKA_API_KEY,
    "KAFKA_API_SECRET": KAFKA_API_SECRET,
    "KAFKA_TOPIC": KAFKA_TOPIC,
    "ALPHA_VANTAGE_URL": ALPHA_VANTAGE_URL,
    "ALPHA_VANTAGE_API_KEY": ALPHA_VANTAGE_API_KEY,
    "ALPHA_VANTAGE_FUNCTION": ALPHA_VANTAGE_FUNCTION,
    # "ALPHA_VANTAGE_FROM_CURRENCY": ALPHA_VANTAGE_FROM_CURRENCY,
    # "ALPHA_VANTAGE_TO_CURRENCY": ALPHA_VANTAGE_TO_CURRENCY,
    "ALPHA_VANTAGE_PAIRS": ALPHA_VANTAGE_PAIRS,

}

missing = [k for k, v in required_vars.items() if not v]
if missing:
    raise EnvironmentError(f"❌ Missing environment variables: {missing}")
