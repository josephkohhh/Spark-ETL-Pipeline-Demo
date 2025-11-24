import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Load the YAML file
config_path = "/Workspace/Repos/kohjoseph97@gmail.com/Spark-ETL-Pipeline-Demo/etl_demo/config/paths.yaml"  
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

raw_path = config['raw_path']
bronze_table_name = config['bronze_table_name']
catalog_name = config['catalog_name']

# Create Spark session
spark = SparkSession.builder.appName("BronzeBatchIngestion").getOrCreate()

# Read all CSVs in the folder
df = spark.read.option("header", True).csv(raw_path)

# Cleanse columns name  
df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

# Add ingestion metadata
df = df.withColumn("ingestion_date", current_timestamp())

# Switch to catalog (use backticks because of hyphens)
spark.sql(f"USE CATALOG `{catalog_name}`")

# Write to bronze Delta table - Bronze is raw data, you mostly want to preserve the data exactly as-is
df.write.format("delta") \
    .mode("append") \
    .saveAsTable(bronze_table_name)


