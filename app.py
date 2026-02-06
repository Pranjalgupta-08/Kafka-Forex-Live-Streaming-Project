import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
from io import BytesIO
import time

# =========================
# CONFIG
# =========================
BUCKET = "forex-kafka-raw1"
PREFIX = "custom-consumer/FOREX_PRANJAL/"
REFRESH_SECONDS = 30
AWS_REGION = "us-east-2"

st.set_page_config(
    page_title="Forex Dashboard",
    layout="wide"
)

st.caption("Kafka → S3 → Spark → Delta → Streamlit")

# S3 Client
s3 = boto3.client("s3", region_name=AWS_REGION)

# =========================
# LOAD GOLD DATA
# =========================
@st.cache_data(ttl=REFRESH_SECONDS)
def load_gold():

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    if "Contents" not in resp:
        return pd.DataFrame()

    files = [
        obj["Key"]
        for obj in resp["Contents"]
        if obj["Key"].endswith(".parquet")
    ]

    dfs = []

    for key in files:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj["Body"].read()
        dfs.append(pd.read_parquet(BytesIO(data)))

    df = pd.concat(dfs, ignore_index=True)

    # =========================
    # RAW → Derived Columns
    # =========================
    df["currency_pair"] = df["from_currency"] + "_" + df["to_currency"]
    df["avg_rate"] = pd.to_numeric(df["exchange_rate"], errors="coerce")
    df["date"] = pd.to_datetime(df["last_refreshed"])

    return df.sort_values("date")


# =========================
# MAIN
# =========================
df = load_gold()

if df.empty:
    st.warning("No data found in gold layer")
    st.stop()


# =========================
# Currency Selector
# =========================
pairs = sorted(df["currency_pair"].unique())

selected_pair = st.selectbox(
    "Select Currency Pair",
    pairs,
    index=0
)

df = df[df["currency_pair"] == selected_pair]

if df.empty:
    st.warning("No data for selected currency")
    st.stop()

# =========================
# TITLE
# =========================
st.title(f"📈 Forex Daily Average ({selected_pair.replace('_',' → ')})")

# =========================
# KPI
# =========================
latest = df.iloc[-1]

k1, k2 = st.columns(2)

k1.metric("💱 Latest Avg Rate", round(latest["avg_rate"], 4))
k2.metric("📅 Last Date", latest["date"].strftime("%Y-%m-%d"))

# =========================
# LINE CHART
# =========================
st.subheader("📉 Daily Average Exchange Rate")

fig = px.line(
    df,
    x="date",
    y="avg_rate",
    markers=True,
    title=f"{selected_pair.replace('_',' → ')} Daily Average"
)

st.plotly_chart(fig, use_container_width=True)

# =========================
# RAW DATA
# =========================
with st.expander("View Raw Gold Data"):
    st.dataframe(df.tail(20), use_container_width=True)

# =========================
# AUTO REFRESH
# =========================
st.caption(f"🔄 Auto refreshing every {REFRESH_SECONDS} seconds")
time.sleep(REFRESH_SECONDS)
st.rerun()
