from google.cloud import bigquery
from pyspark.sql import SparkSession
from apscheduler.schedulers.background import BackgroundScheduler

import websocket
import json
import logging

# Initialize Spark session
spark: SparkSession = SparkSession.builder.appName("WebSocketToBigQuery").getOrCreate()
rdd = spark.sparkContext.emptyRDD()

# Initialize BigQuery client using the service account credentials
bigquery_client = bigquery.Client.from_service_account_json(
    "tubes-big-data-422412-f473452a086a.json"
)

# Define BigQuery dataset and table
dataset_id = "crypto_transaction_indodax"
table_id = "crypto-transaction"
table_ref = bigquery_client.dataset(dataset_id).table(table_id)

# Define schema for the BigQuery table
schema = [
    bigquery.SchemaField("pairs", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("last_price", "FLOAT"),
    bigquery.SchemaField("lowest_price_24h", "FLOAT"),
    bigquery.SchemaField("highest_price_24h", "FLOAT"),
    bigquery.SchemaField("price_at_t_minus_24h", "FLOAT"),
    bigquery.SchemaField("volume_idr_24h", "FLOAT"),
    bigquery.SchemaField("volume_coin_24h", "FLOAT"),
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
    "params": {"channel": "market:summary-24h"},
    "id": 2,
}

def validate_row(row):
    required_keys = [
        "pairs", "timestamp", "last_price", "lowest_price_24h",
        "highest_price_24h", "price_at_t_minus_24h",
        "volume_idr_24h", "volume_coin_24h"
    ]
    for key in required_keys:
        if key not in row:
            return False
        if row[key] is None:
            return False
    return True

def insert_into_bigquery(rows):
    valid_rows = [row for row in rows if validate_row(row)]
    if len(valid_rows) != len(rows):
        print("Some rows were invalid and have not been inserted.")
    if valid_rows:
        errors = bigquery_client.insert_rows_json(table_ref, valid_rows)
        if errors:
            print(f"Errors: {errors}")
        else:
            print(f"{len(valid_rows)} Data inserted into BigQuery successfully!")

def on_message(ws, message):
    global rdd
    print("Received message")
    data = json.loads(message)
    if "result" in data and "data" in data["result"]:
        # Extract the list of data and convert each item to the correct dictionary format
        new_data = [
            {
                "pairs": item[0],
                "timestamp": item[1],
                "last_price": item[2],
                "lowest_price_24h": item[3],
                "highest_price_24h": item[4],
                "price_at_t_minus_24h": item[5],
                "volume_idr_24h": item[6],
                "volume_coin_24h": item[7],
            } for item in data["result"]["data"]["data"]
        ]
        new_rdd = spark.sparkContext.parallelize(new_data)  # Create RDD from the formatted data
        rdd = rdd.union(new_rdd)  # Union the new RDD with the existing one
    else:
        print("Invalid message format: %s", str(message))

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
    print("Inserting RDD to BigQuery")
    if not rdd.isEmpty():
        rows = rdd.collect()  # Collect RDD data into a list of rows
        insert_into_bigquery(rows)  # Insert all collected rows at once
        rdd = spark.sparkContext.emptyRDD()  # Reset RDD after insertion


if __name__ == "__main__":
    # logging.getLogger('apscheduler').setLevel(logging.WARNING)
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
