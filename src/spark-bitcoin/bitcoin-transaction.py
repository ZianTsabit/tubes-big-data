from google.cloud import bigquery
from pyspark.sql import SparkSession
from apscheduler.schedulers.background import BackgroundScheduler

import websocket
import json

# Initialize Spark session
spark: SparkSession = SparkSession.builder.appName("WebSocketToBigQuery").getOrCreate()
rdd = spark.sparkContext.emptyRDD()

# Initialize BigQuery client using the service account credentials
bigquery_client = bigquery.Client.from_service_account_json(
    "tubes-big-data-422412-f473452a086a.json"
)

# Define BigQuery dataset and table
dataset_id = "crypto_transaction_indodax"
table_id = "btc_transaction"
table_ref = bigquery_client.dataset(dataset_id).table(table_id)

# Define schema for the BigQuery table
schema = [
    bigquery.SchemaField("pairs", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("seq_number", "INTEGER"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("volume_idr", "FLOAT"),
    bigquery.SchemaField("volume_btc", "FLOAT"),
]

# Create the BigQuery table if it doesn't exist
try:
    table = bigquery_client.get_table(table_ref)
except:
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)

# Define WebSocket URL
ws_url = "wss://ws3.indodax.com/ws/"

# Define authentication and subscription request messages
auth_message = {
    "params": {
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE5NDY2MTg0MTV9.UR1lBM6Eqh0yWz-PVirw1uPCxe60FdchR8eNVdsskeo"
    },
    "id": 1,
}

subscription_message = {
    "method": 1,
    "params": {"channel": "market:trade-activity-btcidr"},
    "id": 2,
}


def insert_into_bigquery(
    pairs, timestamp, seq_number, action, price, volume_idr, volume_btc
):
    row = {
        "pairs": pairs,
        "timestamp": timestamp,
        "seq_number": seq_number,
        "action": action,
        "price": price,
        "volume_idr": float(volume_idr),
        "volume_btc": float(volume_btc),
    }
    print(row)
    # Insert row into BigQuery
    errors = bigquery_client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"Errors: {errors}")
    else:
        print("Data inserted into BigQuery successfully!")


def on_message(ws, message):
    global rdd
    data = json.loads(message)
    if "result" in data and "data" in data["result"]:
        new_rdd = spark.sparkContext.parallelize(data["result"]["data"]["data"])
        rdd = rdd.union(new_rdd)
    else:
        print("Invalid message format")


def on_error(ws, error):
    print("Error:", error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    print("### opened ###")

    ws.send(json.dumps(auth_message))
    ws.send(json.dumps(subscription_message))


def insert_rdd_to_bigquery():
    global rdd
    if not rdd.isEmpty():
        rdd.foreach(
            lambda x: insert_into_bigquery(
                x["pairs"],
                x["timestamp"],
                x["seq_number"],
                x["action"],
                x["price"],
                x["volume_idr"],
                x["volume_btc"],
            )
        )
        rdd = spark.sparkContext.emptyRDD()


if __name__ == "__main__":
    # Run insert rdd to bigquery every 5 minutes
    scheduler = BackgroundScheduler()
    scheduler.add_job(insert_rdd_to_bigquery, "interval", minutes=5)
    scheduler.start()

    # Connect to WebSocket
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    ws.run_forever()
