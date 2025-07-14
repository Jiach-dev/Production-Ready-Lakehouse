import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# ========================
# Bronze Table: GDELT CSV via Auto Loader
# ========================
@dlt.table(
    comment="Raw GDELT events loaded via Auto Loader"
)
def gdelt_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/schema")
        .load("/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/")
    )

# ========================
# Bronze Table: Wikimedia via API
# ========================
@dlt.table(
    comment="Raw Wikimedia stream via SSE"
)
def wikimedia_bronze():
    from requests import get
    import sseclient

    def stream_wikimedia():
        url = "https://stream.wikimedia.org/v2/stream/recentchange"
        client = sseclient.SSEClient(url)
        for event in client.events():
            yield json.loads(event.data)

    # You'll need to stream into a temp table or a memory stream using UDTF or generator
    # For this challenge, simulate ingestion (or document in the README)

    raise NotImplementedError("Wikimedia API ingestion via UDTF should be implemented here.")

# ========================
# Silver Tables (Parse & Clean)
# ========================
@dlt.table
def gdelt_silver():
    return dlt.read("gdelt_bronze").select(
        col("_c0").alias("GLOBALEVENTID").cast("long"),
        col("_c1").alias("EventCode"),
        col("_c7").alias("Actor1Name"),
        col("_c52").alias("ActionGeo_Fullname"),
        col("_c60").alias("SOURCEURL")
    ).withColumn("event_time", current_timestamp())

@dlt.table
def wikimedia_silver():
    return dlt.read("wikimedia_bronze").selectExpr(
        "title", "user", "timestamp", "wiki"
    ).filter("wiki = 'enwiki'").withColumn("event_time", current_timestamp())

# ========================
# Gold Table: Correlated Stream-Stream Join
# ========================
@dlt.table
def correlated_gold_events():
    g = dlt.read_stream("gdelt_silver").withWatermark("event_time", "1 hour")
    w = dlt.read_stream("wikimedia_silver").withWatermark("event_time", "1 hour")

    return g.join(w, expr("""
        g.ActionGeo_Fullname = w.title AND
        g.event_time BETWEEN w.event_time - INTERVAL 1 HOUR AND w.event_time + INTERVAL 1 HOUR
    """))
