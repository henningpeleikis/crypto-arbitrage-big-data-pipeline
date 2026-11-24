# Run this Spark app with the following commands
# export PYSPARK_PYTHON=/home/osbdet/.jupyter_venv/bin/python3

# export SPARK_HOME=/home/osbdet/.jupyter_venv/lib/python3.11/site-packages/pyspark

# $SPARK_HOME/bin/spark-submit   --master local[*]   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4   2_Arbitrage_Consuming.py 2>&1 | grep -v INFO


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Fee Tiers Structure (Based on 2025 Binance & Coinbase documentation)
# Sources: Binance VIP Program, Coinbase Advanced Trade tiers
FEE_TIERS = {
    'STANDARD': {
        'binance': 0.0010,    # 0.10% (default retail)
        'coinbase': 0.0050,   # 0.50% average (0.4% maker + 0.6% taker)
        'total_pct': 0.60,
        'description': 'Retail users (default tier)'
    },
    'ACTIVE': {
        'binance': 0.00095,   # 0.095% (VIP 1: 0.09% maker + 0.1% taker avg)
        'coinbase': 0.00325,  # 0.325% (Advanced 1: 0.25% maker + 0.4% taker avg)
        'total_pct': 0.42,
        'description': 'Active traders (≥$1M Binance, ≥$10K Coinbase monthly volume)'
    },
    'INSTITUTIONAL': {
        'binance': 0.00030,   # 0.03% (VIP 5: 0.02% maker + 0.04% taker avg)
        'coinbase': 0.00060,  # 0.06% (Advanced 6: 0.02% maker + 0.1% taker avg)
        'total_pct': 0.09,
        'description': 'Institutional (≥$150M Binance, ≥$50M Coinbase monthly volume)'
    },
    'P2P': {
        'binance': 0.00000,   # 0% (P2P taker fee is 0%)
        'coinbase': 0.00000,   # 0% (Coinbase has no P2P, but for educational purposes we also use 0)
        'total_pct': 0.00,
        'description': 'P2P Trading (Binance P2P only - 0% taker fee)'
    }
}

print("=" * 80)
print("MULTI-TIER FEE MODEL INITIALIZED")
print("=" * 80)
for tier_name, tier_data in FEE_TIERS.items():
    print(f"\n{tier_name}:")
    print(f"  Binance: {tier_data['binance']*100:.3f}%")
    print(f"  Coinbase: {tier_data['coinbase']*100:.3f}%")
    print(f"  Total: {tier_data['total_pct']:.2f}%")
    print(f"  Description: {tier_data['description']}")
print("\n" + "=" * 80)

import os

# Set Kafka package before starting Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4" pyspark-shell'

print("Kafka package configured")

# Start Spark Session with Kafka support
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Arbitrage_Fees") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Read arbitrage signals from Kafka
signals_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "crypto.arb.signals")
        .load()
)

# Parse JSON from Kafka
signals_df = signals_raw.selectExpr("CAST(value AS STRING) as json")

# Define schema
schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("asset", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("best_spread", DoubleType(), True),
    StructField("spread_buy_binance_sell_coinbase", DoubleType(), True),
    StructField("spread_buy_coinbase_sell_binance", DoubleType(), True),
    StructField("binance_bid", DoubleType(), True),
    StructField("binance_ask", DoubleType(), True),
    StructField("coinbase_bid", DoubleType(), True),
    StructField("coinbase_ask", DoubleType(), True),
])

signals_parsed = signals_df.select(F.from_json("json", schema).alias("d")).select("d.*")

print("Connected to Kafka and reading signals from crypto.arb.signals")

from pyspark.sql import functions as F

def add_profit_cols(df, fee_binance, fee_coinbase, tier_name):
    # pick buy/sell prices based on direction
    buy_price  = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", F.col("binance_ask")) \
                  .otherwise(F.col("coinbase_ask"))
    sell_price = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", F.col("coinbase_bid")) \
                  .otherwise(F.col("binance_bid"))

    # fees depend on which exchange is used on each leg
    buy_fee = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", buy_price * F.lit(fee_binance)) \
               .otherwise(buy_price * F.lit(fee_coinbase))
    sell_fee = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", sell_price * F.lit(fee_coinbase)) \
                .otherwise(sell_price * F.lit(fee_binance))

    gross_profit = sell_price - buy_price
    total_fees = buy_fee + sell_fee
    net_profit = gross_profit - total_fees

    # exchanges for display
    buy_ex = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", F.lit("Binance")).otherwise(F.lit("Coinbase"))
    sell_ex = F.when(F.col("direction")=="BUY_BINANCE_SELL_COINBASE", F.lit("Coinbase")).otherwise(F.lit("Binance"))

    return (df
        .withColumn("tier", F.lit(tier_name))
        .withColumn("buy_exchange", buy_ex)
        .withColumn("sell_exchange", sell_ex)
        .withColumn("buy_price", buy_price)
        .withColumn("sell_price", sell_price)
        .withColumn("gross_profit", gross_profit)
        .withColumn("total_fees", total_fees)
        .withColumn("net_profit", net_profit)
        .withColumn("action", F.when(F.col("net_profit") > 0, F.lit("EXECUTE")).otherwise(F.lit("SKIP")))
    )

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"\n{'='*100}")
    print(f"BATCH {batch_id} - evaluating opportunities")
    print(f"{'='*100}")

    # Build one DF per tier, then union them
    tier_dfs = []
    for tier_name, fees in FEE_TIERS.items():
        tier_dfs.append(add_profit_cols(batch_df, fees["binance"], fees["coinbase"], tier_name))

    all_tiers = tier_dfs[0]
    for d in tier_dfs[1:]:
        all_tiers = all_tiers.unionByName(d)

    # Only keep profitable rows for printing (and cap!)
    to_print = (all_tiers
        .filter(F.col("action") == "EXECUTE")
        .orderBy(F.col("net_profit").desc())
        .limit(50)  # keep prints manageable
        .collect()
    )

    if not to_print:
        print("No profitable trades in this batch.")
        return

    # Pretty printing (your style)
    for r in to_print:
        print(f"\n{'─'*100}")
        print(f"🔹 [{r['tier']}] ASSET: {r['asset']} | DIRECTION: {r['direction']}")
        print(f"{'─'*100}")
        print(f"  🟢 EXECUTE")
        print(f"     BUY  @ {r['buy_exchange']:8}: ${r['buy_price']:,.2f}")
        print(f"     SELL @ {r['sell_exchange']:8}: ${r['sell_price']:,.2f}")
        print(f"     Gross: ${r['gross_profit']:.4f} | Fees: ${r['total_fees']:.4f}")
        print(f"     NET PROFIT: ${r['net_profit']:.4f}")


# This connects to Kafka, reads arbitrage signals, and processes them in real-time
query = (
    signals_parsed  # Start with the parsed signals DataFrame from Cell 4
        .writeStream  # Create a streaming query
        .foreachBatch(process_batch)  # Call our function for each batch of data
        .outputMode("append")  # Only process new data (append mode)
        .start()  # Start the query (begins monitoring Kafka)
)

# confirmation messages
print("\n" + "="*100)
print("MULTI-TIER STREAMING QUERY STARTED")
print("="*100)
print("Arbitrage opportunities across 4 fee tiers:")
print("   • STANDARD: Retail (0.60% total fees)")
print("   • ACTIVE: Active traders (0.42% total fees)")
print("   • INSTITUTIONAL: Institutional (0.09% total fees)")
print("   • P2P: Binance P2P (0% total fees)")
print("\n Monitoring crypto.arb.signals Kafka topic...")
print(" To stop: query.stop()")
print("="*100)

# this is added for the Spark application to keep running forever until terminated
print("Streaming started. Awaiting termination...")
query.awaitTermination() 