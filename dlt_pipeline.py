# Databricks notebook source
# MAGIC %pip install --quiet sentence-transformers
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC # Download model before initializing Spark
# MAGIC from sentence_transformers import SentenceTransformer
# MAGIC import os
# MAGIC
# MAGIC # Create cache directory in DBFS (persistent storage)
# MAGIC cache_dir = "/dbfs/tmp/sentence_transformers" 
# MAGIC os.makedirs(cache_dir, exist_ok=True)
# MAGIC
# MAGIC # Download model with error handling
# MAGIC try:
# MAGIC     print("Downloading model...")
# MAGIC     model = SentenceTransformer(
# MAGIC         'all-MiniLM-L6-v2',
# MAGIC         cache_folder=cache_dir,
# MAGIC         device='cpu'
# MAGIC     )
# MAGIC     print("Model downloaded successfully!")
# MAGIC except Exception as e:
# MAGIC     print(f"Model download failed: {str(e)}")
# MAGIC     # Try alternative approach if direct download fails
# MAGIC     try:
# MAGIC         from huggingface_hub import snapshot_download
# MAGIC         snapshot_download(
# MAGIC             "sentence-transformers/all-MiniLM-L6-v2",
# MAGIC             cache_dir=cache_dir
# MAGIC         )
# MAGIC         model = SentenceTransformer(
# MAGIC             'all-MiniLM-L6-v2',
# MAGIC             cache_folder=cache_dir,
# MAGIC             device='cpu'
# MAGIC         )
# MAGIC         print("Model downloaded via alternative method!")
# MAGIC     except Exception as alt_e:
# MAGIC         print(f"Alternative download also failed: {str(alt_e)}")
# MAGIC         raise

# COMMAND ----------

from pyspark.sql import SparkSession
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import json
from datetime import datetime
from pyspark.sql.streaming import StreamingQueryListener

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Model configuration - must match Cell 1 location
cache_dir = "/dbfs/tmp/sentence_transformers"

# Verify model files exist
if not os.path.exists(f"{cache_dir}/sentence-transformers_all-MiniLM-L6-v2"):
    raise FileNotFoundError("Model files not found in cache directory")

class StreamingMetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        start_metrics = {
            "event_type": "query_started",
            "query_id": event.id,
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"STREAMING_EVENT: {json.dumps(start_metrics)}")
    
    def onQueryProgress(self, event):
        progress_metrics = {
            "event_type": "query_progress",
            "query_id": event.progress.id,
            "input_rows": event.progress.numInputRows,
            "duration_ms": event.progress.durationMs.get("triggerExecution"),
            "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        }
        print(f"STREAMING_METRICS: {json.dumps(progress_metrics)}")
    
    def onQueryTerminated(self, event):
        end_metrics = {
            "event_type": "query_terminated",
            "query_id": event.id,
            "exception": str(event.exception) if event.exception else None
        }
        print(f"STREAMING_EVENT: {json.dumps(end_metrics)}")

spark.streams.addListener(StreamingMetricsListener())

# COMMAND ----------

# GDELT Bronze Schema
gdelt_bronze_schema = StructType([
    StructField("GLOBALEVENTID", StringType()),
    StructField("Day", StringType()),
    StructField("MonthYear", StringType()),
    StructField("Year", StringType()),
    StructField("FractionDate", StringType()),
    StructField("Actor1Code", StringType()),
    StructField("Actor1Name", StringType()),
    StructField("Actor1CountryCode", StringType()),
    StructField("Actor1KnownGroupCode", StringType()),
    StructField("Actor1EthnicCode", StringType()),
    StructField("Actor1Religion1Code", StringType()),
    StructField("Actor1Religion2Code", StringType()),
    StructField("Actor1Type1Code", StringType()),
    StructField("Actor1Type2Code", StringType()),
    StructField("Actor1Type3Code", StringType()),
    StructField("Actor2Code", StringType()),
    StructField("Actor2Name", StringType()),
    StructField("Actor2CountryCode", StringType()),
    StructField("Actor2KnownGroupCode", StringType()),
    StructField("Actor2EthnicCode", StringType()),
    StructField("Actor2Religion1Code", StringType()),
    StructField("Actor2Religion2Code", StringType()),
    StructField("Actor2Type1Code", StringType()),
    StructField("Actor2Type2Code", StringType()),
    StructField("Actor2Type3Code", StringType()),
    StructField("IsRootEvent", StringType()),
    StructField("EventCode", StringType()),
    StructField("EventBaseCode", StringType()),
    StructField("EventRootCode", StringType()),
    StructField("QuadClass", StringType()),
    StructField("GoldsteinScale", DoubleType()),
    StructField("NumMentions", IntegerType()),
    StructField("NumSources", IntegerType()),
    StructField("NumArticles", IntegerType()),
    StructField("AvgTone", DoubleType()),
    StructField("Actor1Geo_Type", StringType()),
    StructField("Actor1Geo_Fullname", StringType()),
    StructField("Actor1Geo_CountryCode", StringType()),
    StructField("Actor1Geo_ADM1Code", StringType()),
    StructField("Actor1Geo_Lat", DoubleType()),
    StructField("Actor1Geo_Long", DoubleType()),
    StructField("Actor1Geo_FeatureID", StringType()),
    StructField("Actor2Geo_Type", StringType()),
    StructField("Actor2Geo_Fullname", StringType()),
    StructField("Actor2Geo_CountryCode", StringType()),
    StructField("Actor2Geo_ADM1Code", StringType()),
    StructField("Actor2Geo_Lat", DoubleType()),
    StructField("Actor2Geo_Long", DoubleType()),
    StructField("Actor2Geo_FeatureID", StringType()),
    StructField("ActionGeo_Type", StringType()),
    StructField("ActionGeo_Fullname", StringType()),
    StructField("ActionGeo_CountryCode", StringType()),
    StructField("ActionGeo_ADM1Code", StringType()),
    StructField("ActionGeo_Lat", DoubleType()),
    StructField("ActionGeo_Long", DoubleType()),
    StructField("ActionGeo_FeatureID", StringType()),
    StructField("DATEADDED", StringType()),
    StructField("SOURCEURL", StringType()),
    StructField("event_time", TimestampType()),
    StructField("embedding", ArrayType(FloatType()))
])

# GDELT Silver Schema
gdelt_silver_schema = StructType([
    StructField("EventID", StringType()),
    StructField("EventCode", StringType()),
    StructField("Actor1Name", StringType()),
    StructField("ActionGeo_Fullname", StringType()),
    StructField("SOURCEURL", StringType()),
    StructField("event_time", TimestampType()),
    StructField("gdelt_text", StringType()),
    StructField("embedding", ArrayType(FloatType()))
])

# Wikimedia Silver Schema
wikimedia_silver_schema = StructType([
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("wiki", StringType()),
    StructField("parsedcomment", StringType()),
    StructField("event_time", TimestampType()),
    StructField("wiki_text", StringType()),
    StructField("embedding", ArrayType(FloatType()))
])

# COMMAND ----------

@dlt.table(
    name="gdelt_bronze",
    comment="GDELT raw events via Auto Loader from CSV volume"
)
def load_gdelt_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("pathGlobFilter", "*.csv")
        .option("header", "false")
        .schema(gdelt_bronze_schema)
        .option("cloudFiles.schemaLocation", "/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/schema")
        .load("/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/")
    )

@dlt.table(
    comment="Wikimedia stream as Delta table from simulation job"
)
def wikimedia_bronze_2():
    return spark.readStream.table("prod_lakehouse.wikigdelt_schema.wikimedia_bronze")

# COMMAND ----------

def get_sentence_transformer():
    """Helper function to load model with proper caching"""
    return SentenceTransformer(
        'all-MiniLM-L6-v2',
        cache_folder=cache_dir,
        device='cpu'
    )

def embed_gdelt_chunk(pdf):
    try:
        model = get_sentence_transformer()
        pdf['gdelt_text'] = pdf['Actor1Name'].fillna('') + ' ' + \
                          pdf['EventCode'].fillna('') + ' ' + \
                          pdf['ActionGeo_Fullname'].fillna('')
        pdf['embedding'] = pdf['gdelt_text'].apply(
            lambda x: model.encode(x).tolist() if x else None
        )
        return pdf[list(gdelt_silver_schema.fieldNames())]
    except Exception as e:
        print(f"Error processing GDELT chunk: {str(e)}")
        return pd.DataFrame(columns=gdelt_silver_schema.fieldNames())

@dlt.table(
    comment="Cleaned and parsed GDELT data with embeddings"
)
def gdelt_silver():
    df = dlt.read_stream("gdelt_bronze")
    return (df.select(
        col("GLOBALEVENTID").alias("EventID"),
        col("EventCode"),
        col("Actor1Name"),
        col("ActionGeo_Fullname"),
        col("SOURCEURL"),
        current_timestamp().alias("event_time")
    ).mapInPandas(embed_gdelt_chunk, schema=gdelt_silver_schema))

# COMMAND ----------

def embed_wikimedia_chunk(pdf):
    try:
        model = get_sentence_transformer()
        pdf['wiki_text'] = pdf['title'].fillna('') + ' ' + pdf['parsedcomment'].fillna('')
        pdf['embedding'] = pdf['wiki_text'].apply(
            lambda x: model.encode(x).tolist() if x else None
        )
        return pdf[list(wikimedia_silver_schema.fieldNames())]
    except Exception as e:
        print(f"Error processing Wikimedia chunk: {str(e)}")
        return pd.DataFrame(columns=wikimedia_silver_schema.fieldNames())

@dlt.table(
    comment="Cleaned Wikimedia data with embeddings",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def wikimedia_silver():
    return (
        dlt.read_stream("wikimedia_bronze_2")
        .select(
            col("title"),
            col("user"),
            col("timestamp").cast("timestamp"),
            col("wiki"),
            col("parsedcomment"),
            current_timestamp().alias("event_time")
        )
        .filter(col("wiki") == "enwiki")
        .mapInPandas(embed_wikimedia_chunk, schema=wikimedia_silver_schema)
    )

# COMMAND ----------

@pandas_udf(DoubleType())
def cosine_similarity(v1, v2):
    import numpy as np
    if v1 is None or v2 is None:
        return None
    a, b = np.array(v1), np.array(v2)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))

@dlt.table(
    comment="Semantically correlated GDELT and Wikimedia events"
)
def correlated_gold_events():
    g = (dlt.read_stream("gdelt_silver")
        .withWatermark("event_time", "4 hour")
        .withColumn("time_bucket", date_trunc("hour", col("event_time")))
    
    w = (dlt.read_stream("wikimedia_silver")
        .withWatermark("event_time", "4 hour")
        .withColumn("time_bucket", date_trunc("hour", col("event_time")))

    return (g.join(w, "time_bucket")
        .withColumn("cosine_sim", cosine_similarity(col("g.embedding"), col("w.embedding")))
        .filter(col("cosine_sim") > 0.05)
        .select(
            col("g.EventID"),
            col("g.EventCode"),
            col("g.Actor1Name"),
            col("g.ActionGeo_Fullname"),
            col("g.SOURCEURL"),
            col("w.title"),
            col("w.user"),
            col("w.wiki"),
            col("cosine_sim"),
            col("g.event_time").alias("event_time")
        ))