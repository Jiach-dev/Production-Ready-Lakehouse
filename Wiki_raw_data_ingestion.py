# Databricks notebook source
# simulate_wikimedia_stream.py

import requests
import sseclient
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create or get the Spark session
spark = SparkSession.builder.getOrCreate()

# Stream URL
url = "https://stream.wikimedia.org/v2/stream/recentchange"
client = sseclient.SSEClient(url)

# Infinite stream ingestion
for event in client:
    if event.event == "message":
        try:
            data = json.loads(event.data)

            # Flatten selected fields from nested structure
            flat_data = {
                "title": data.get("title"),
                "user": data.get("user"),
                "timestamp": data.get("timestamp"),
                "wiki": data.get("wiki"),
                "type": data.get("type"),
                "bot": data.get("bot"),
                "server_name": data.get("server_name"),
                "server_url": data.get("server_url"),
                "parsedcomment": data.get("parsedcomment"),
                "meta_uri": data.get("meta", {}).get("uri"),
                "meta_id": data.get("meta", {}).get("id"),
                "meta_dt": data.get("meta", {}).get("dt"),
                "meta_stream": data.get("meta", {}).get("stream"),
                "event_time": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data.get("timestamp", 0)))
            }

            # Convert to DataFrame
            df = spark.createDataFrame([Row(**flat_data)])

            # Append to Delta table in catalog
            df.write.mode("append").format("delta").saveAsTable("prod_lakehouse.wikigdelt_schema.wikimedia_bronze")

        except Exception as e:
            print(f"Error: {e}")
    
    time.sleep(1)
