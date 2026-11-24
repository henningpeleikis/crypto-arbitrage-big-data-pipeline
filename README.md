# Crypto Arbitrage & Forecasting — Big Data Pipeline

An end-to-end big data architecture with two paths:

- **Streaming:** live bid/ask prices from **Binance** and **Coinbase** flow through **Apache NiFi → Kafka → Spark Structured Streaming**, which aligns the two exchanges in real time and publishes arbitrage signals to a Kafka topic.
- **Batch:** a daily job pulls 1,000 days of price history for **BTC, ETH, SOL, XRP and BNB** into a **MinIO** data lake, trains several **Spark ML** regressors plus **ARIMA**, keeps the best model in a model registry and writes 7-day forecasts to **MariaDB**.

`Apache NiFi` · `Apache Kafka` · `Spark Structured Streaming` · `Spark MLlib` · `MinIO (S3)` · `MariaDB` · `statsmodels` · `Python`

## Architecture

```
STREAMING
 Binance API ─┐                      ┌─ crypto.prices.binance ─┐
              ├─ NiFi (poll + JSON) ─┤                         ├─ Spark: 1_Arbitrage_Processing ─ crypto.arb.signals ─ Spark: 2_Arbitrage_Consuming
 Coinbase API ┘                      └─ crypto.prices.coinbase ┘     (normalise, dedupe, join, spread)                (monitor / alert)

BATCH (daily)
 Binance history ─ NiFi ─ MinIO: raw-crypto-data ─ Spark ML (LinearRegression, RandomForest, GBT) + ARIMA
                                                     │                         │
                                         MinIO: model-registry        MariaDB: machine_learning_forecasts
                                         (best model binaries)                 arima_forecasts (7-day horizon)
```

### Streaming details
- Each exchange's payload is parsed into one schema (`exchange, base, quote, bid, ask, event_ts`): symbol formats (`BTC-USD` vs `BTCUSDT`) are normalised, timezones fixed and malformed NiFi JSON repaired on the fly.
- Duplicate events are dropped per exchange and timestamp so a signal isn't counted twice.
- Signals land on the `crypto.arb.signals` topic and are consumed by a second Spark job.

### Batch details
- Features engineered with technical indicators (`pandas_ta`) and window functions.
- Several regressors are compared on held-out data; the winner is saved to MinIO and used for the next 7 days of predictions.
- ARIMA forecasts are stored next to them so the two approaches can be compared.

## Repository layout

```
streaming/
  nifi_streaming_binance_coinbase.json   NiFi flow (import as a template)
  1_Arbitrage_Processing.{py,ipynb}      Spark job: Kafka in -> arbitrage signals out
  2_Arbitrage_Consuming.{py,ipynb}       Spark job: consume and display signals
batch/
  nifi_crypto_historical_timeseries.json NiFi flow for historical prices
  ml_and_arima_batch_job.ipynb           Spark ML + ARIMA training and forecasting
  create_*_forecasts.sql                 MariaDB schema
```

## Running it

Built and tested on the OSBDET big-data VM (Jupyter, NiFi, Kafka, MinIO and MariaDB on localhost).

1. Create MinIO buckets `raw-crypto-data` and `model-registry`.
2. Create the database tables:
   ```bash
   mariadb -u osbdet -p < batch/create_machine_learning_forecasts.sql
   mariadb -u osbdet -p < batch/create_arima_forecasts.sql
   ```
3. Set credentials as environment variables (they are not stored in the code):
   ```bash
   export MARIADB_PASSWORD=...
   export MINIO_ACCESS_KEY=...
   export MINIO_SECRET_KEY=...
   ```
4. Import and start the NiFi flows, then run the batch notebook or submit the streaming jobs:
   ```bash
   $SPARK_HOME/bin/spark-submit --master local[*] \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
     streaming/1_Arbitrage_Processing.py
   ```

The `.py` files contain the same code as the notebooks, packaged to run as Spark applications.

## Context

Group project for *Modern Data Architecture for Big Data II* (Master in Business Analytics & Data Science, IE University).
