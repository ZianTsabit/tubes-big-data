from google.cloud import bigquery
from pyspark.sql import SparkSession

import websocket
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WebSocketToBigQuery") \
    .getOrCreate()

# Initialize BigQuery client using the service account credentials
bigquery_client = bigquery.Client.from_service_account_json('tubes-big-data-422412-f473452a086a.json')

# Define BigQuery dataset and table
dataset_id = 'crypto_transaction_indodax'
table_id = 'crypto-transaction'
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
    "id": 1
}

subscription_message = {
    "method": 1,
    "params": {
        "channel": "market:summary-24h"
    },
    "id": 2
}

def insert_into_bigquery(pairs, timestamp, last_price, lowest_price, highest_price,
                         price_at_t_minus_24h, volume_idr_24h, volume_coin_24h):
    row = {
        "pairs": pairs,
        "timestamp": timestamp,
        "last_price": last_price,
        "lowest_price_24h": lowest_price,
        "highest_price_24h": highest_price,
        "price_at_t_minus_24h": price_at_t_minus_24h,
        "volume_idr_24h": volume_idr_24h,
        "volume_coin_24h": volume_coin_24h
    }
    print(row)
    # Insert row into BigQuery
    errors = bigquery_client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"Errors: {errors}")
    else:
        print("Data inserted into BigQuery successfully!")

def on_message(ws, message):
    data = json.loads(message)
    if 'result' in data and 'data' in data['result']:
        for item in data['result']['data']['data']:
            insert_into_bigquery(*item)
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

if __name__ == "__main__":
    # Connect to WebSocket
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    ws.on_open = on_open
    ws.run_forever()