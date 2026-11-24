# Run this Spark app with the following commands
# export PYSPARK_PYTHON=/home/osbdet/.jupyter_venv/bin/python3

# export SPARK_HOME=/home/osbdet/.jupyter_venv/lib/python3.11/site-packages/pyspark

# $SPARK_HOME/bin/spark-submit   --master local[*]   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4   1_Arbitrage_Processing.py 2>&1 | grep -v INFO

import os
from pyspark.sql.session import SparkSession

spark_session = \
  SparkSession.builder\
              .appName("Arbitrage opportunity finder")\
              .getOrCreate()

print(f"This cluster relies on Spark '{spark_session.version}'")

import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Read Coinbase Kafka messages
coinbase_raw = (
    spark_session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "crypto.prices.coinbase")
      .option("startingOffsets", "latest")
      .load()
      .select(F.col("value").cast("string").alias("json_str"))
)

# Coinbase payload (expected). If NiFi standardizes Coinbase to the same format as Binance,
# this schema still works because we use robust extraction below.
coinbase_schema = StructType([
    StructField("symbol", StringType(), True),        # e.g. BTC-USD
    StructField("bid", StringType(), True),
    StructField("ask", StringType(), True),
    StructField("bidPrice", StringType(), True),      # fallback if using Binance-like keys
    StructField("askPrice", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("time", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# Parse JSON
coinbase_parsed0 = (
    coinbase_raw
      .select(from_json(col("json_str"), coinbase_schema).alias("d"))
      .select("d.*")
)

# Choose bid/ask fields robustly (bid/ask OR bidPrice/askPrice)
coinbase_bid = F.coalesce(col("bid"), col("bidPrice"))
coinbase_ask = F.coalesce(col("ask"), col("askPrice"))

# Choose timestamp field robustly
coinbase_event = F.coalesce(col("event_time"), col("ts"), col("time"), col("timestamp"))

# Normalize timezone +01:00 -> +0100 so Spark parses reliably
coinbase_event_norm = F.regexp_replace(coinbase_event, r"([+-]\d\d):(\d\d)$", r"\1\2")

coinbase_unified = (
    coinbase_parsed0
      .withColumn("exchange", F.lit("coinbase")) # Adds a new column 'exchange' and sets it to 'coinbase'
      .withColumn("symbol", col("symbol")) # Checks that the 'symbol' column is present
      .withColumn("base", F.split(col("symbol"), "-").getItem(0)) # Extracts the base currency (e.g., 'BTC' from 'BTC-USD')
      .withColumn("quote", F.split(col("symbol"), "-").getItem(1)) # Extracts the quote currency (e.g., 'USD' from 'BTC-USD')
      .withColumn("bid", coinbase_bid.cast("double")) # Casts bid price to double
      .withColumn("ask", coinbase_ask.cast("double")) # Casts ask price to double
      .withColumn("event_ts", F.to_timestamp(coinbase_event_norm)) # Converts normalized timestamp string to proper timestamp type
      .select("exchange", "base", "quote", "bid", "ask", "event_ts") # Selects and orders the final set of columns for a unified schema
      .filter(col("event_ts").isNotNull() & (col("base") != "")) # Filter out rows where timestamp is null or base is empty
      .filter(col("bid").isNotNull() & col("ask").isNotNull()) # Filter out rows where bid or ask price is null
      .dropDuplicates(["exchange", "base", "quote", "event_ts"]) # Exactly once processing by dropping duplicates based on these columns within the stream
)

import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

binance_raw = (
    spark_session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "crypto.prices.binance")
      .option("startingOffsets", "latest")
      .load()
      .select(F.col("value").cast("string").alias("json_str"))
)

# Binance payload produced by NiFi (based on your sample)
binance_schema = StructType([
    StructField("symbol", StringType(), True),      # e.g. BTCUSDT
    StructField("bidPrice", StringType(), True),
    StructField("bidQty", StringType(), True),
    StructField("askPrice", StringType(), True),
    StructField("askQty", StringType(), True),
    StructField("event_time", StringType(), True),  # should be quoted string; we fix if not
])

# Fix invalid JSON if NiFi accidentally emits unquoted event_time:
#  "event_time":2025-...+01:00  ->  "event_time":"2025-...+01:00"
binance_fixed = binance_raw.withColumn(
    "json_fixed",
    F.regexp_replace(
        col("json_str"),
        r'"event_time"\s*:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}T[^",}]+)',
        r'"event_time":"\1"'
    )
)

binance_parsed = (
    binance_fixed
      .select(from_json(col("json_fixed"), binance_schema).alias("d"))
      .select("d.*")
)

# Normalize timezone +01:00 -> +0100 for reliable parsing
binance_event_norm = F.regexp_replace(col("event_time"), r"([+-]\d\d):(\d\d)$", r"\1\2")

binance_unified = (
    binance_parsed
      .withColumn("exchange", F.lit("binance")) # Add the 'exchange' column and set it to 'binance'
      # assumes all your binance pairs are *USDT; adjust if you use others
      .withColumn("base", F.regexp_extract(col("symbol"), r"^(.+?)USDT$", 1)) # Extracts the base currency (e.g., 'BTC' from 'BTCUSDT')
      .withColumn("quote", F.lit("USDT"))      # Sets the quote currency to USDT
      .withColumn("bid", col("bidPrice").cast("double")) # Casts bidPrice to double
      .withColumn("ask", col("askPrice").cast("double")) # Casts askPrice to double
      .withColumn("event_ts", F.to_timestamp(binance_event_norm)) # Converts normalized timestamp to proper timestamp type
      .select("exchange", "base", "quote", "bid", "ask", "event_ts") # Selects and orders the final set of columns for a unified schema
      .filter(col("event_ts").isNotNull() & (col("base") != "")) # Filters out rows where event_ts is null or base is empty
      .filter(col("bid").isNotNull() & col("ask").isNotNull()) # Filters out rows where bid or ask price is null
      .dropDuplicates(["exchange", "base", "quote", "event_ts"]) # Exactly once processing by dropping duplicates based on these columns within the stream
)

import pyspark.sql.functions as F
from pyspark.sql.functions import col

# --- Config ---
WATERMARK = "10 seconds"          # tolerate late data
WINDOW = "1 second"               # compute opportunities each second

# Combine both exchanges into one stream
ticks = binance_unified.unionByName(coinbase_unified)

# 1) Snapshot per second per (base, exchange) using the latest tick in that second
per_exch_1s = (
    ticks
      .withWatermark("event_ts", WATERMARK)
      .groupBy(
          F.window(col("event_ts"), WINDOW).alias("w"),
          col("base").alias("asset"),
          col("exchange")
      )
      .agg(
          F.max_by(col("bid"), col("event_ts")).alias("bid"),
          F.max_by(col("ask"), col("event_ts")).alias("ask"),
          F.max(col("event_ts")).alias("last_event_ts")
      )
)

# 2) Pivot so each row has both exchanges side-by-side (binance_* and coinbase_*)
pivoted = (
    per_exch_1s
      .groupBy("w", "asset")
      .pivot("exchange", ["binance", "coinbase"])
      .agg(
          F.first("bid").alias("bid"),
          F.first("ask").alias("ask")
      )
)

# After pivot, columns look like: binance_bid, binance_ask, coinbase_bid, coinbase_ask
arb_simple = (
    pivoted
      .withColumnRenamed("binance_bid", "binance_bid")
      .withColumnRenamed("binance_ask", "binance_ask")
      .withColumnRenamed("coinbase_bid", "coinbase_bid")
      .withColumnRenamed("coinbase_ask", "coinbase_ask")
      .filter(col("binance_bid").isNotNull() & col("binance_ask").isNotNull()
              & col("coinbase_bid").isNotNull() & col("coinbase_ask").isNotNull())
      .withColumn("ts", col("w.end"))
      # Spread = sell_bid - buy_ask
      .withColumn("spread_buy_binance_sell_coinbase", col("coinbase_bid") - col("binance_ask"))
      .withColumn("spread_buy_coinbase_sell_binance", col("binance_bid") - col("coinbase_ask"))
      .withColumn(
          "best_spread",
          F.greatest(col("spread_buy_binance_sell_coinbase"), col("spread_buy_coinbase_sell_binance"))
      )
      .withColumn(
          "direction",
          F.when(col("spread_buy_binance_sell_coinbase") >= col("spread_buy_coinbase_sell_binance"),
                 F.lit("BUY_BINANCE_SELL_COINBASE"))
           .otherwise(F.lit("BUY_COINBASE_SELL_BINANCE"))
      )
      .select(
          "ts", "asset", "direction", "best_spread",
          "spread_buy_binance_sell_coinbase",
          "spread_buy_coinbase_sell_binance",
          "binance_bid", "binance_ask",
          "coinbase_bid", "coinbase_ask"
      )
)

# Optional: console debug
# arb_simple.writeStream.format("console").outputMode("append").option("truncate","false").start()
import pyspark.sql.functions as F
from pyspark.sql.functions import col

arb_out = arb_simple.select(
    F.to_json(F.struct(
        col("ts"),
        col("asset"),
        col("direction"),
        col("best_spread"),
        col("spread_buy_binance_sell_coinbase"),
        col("spread_buy_coinbase_sell_binance"),
        col("binance_bid"), col("binance_ask"),
        col("coinbase_bid"), col("coinbase_ask"),
    )).alias("value")
)

kafka_q = (
    arb_out.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "crypto.arb.signals")
      .option("checkpointLocation", "/tmp/spark-checkpoints/crypto-arb-signals-min-9")
      .outputMode("append")
      .start()
)

# this is added for the Spark application to keep running forever until terminated
print("Streaming started. Awaiting termination...")
kafka_q.awaitTermination() 
