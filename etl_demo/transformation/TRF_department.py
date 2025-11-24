import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date


# Load YAML config
config_path = "/Workspace/Repos/kohjoseph97@gmail.com/Spark-ETL-Pipeline-Demo/etl_demo/config/paths.yaml"
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

bronze_table_name = config['bronze_table_name']
silver_table_name = config['silver_table_name']
catalog_name = config['catalog_name']

# Create Spark session
spark = SparkSession.builder.appName("BronzeToSilverTransformation").getOrCreate()

# Switch to catalog (use backticks because of hyphens)
spark.sql(f"USE CATALOG `{catalog_name}`")

# Read bronze table
df_bronze = spark.table(bronze_table_name)

# Transform
# - remove duplicates
# - cast to proper types
# - add trf meta data
df_silver = (
    df_bronze
    .dropDuplicates()
    .withColumn("revenue", col("revenue").cast("double"))
    .withColumn("revenue_goal", col("revenue_goal").cast("double"))
    .withColumn("margin", col("margin").cast("double"))
    .withColumn("margin_goal", col("margin_goal").cast("double"))
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))  # adjust format if needed
    .withColumn("sales_quantity", col("sales_quantity").cast("int"))
    .withColumn("customers", col("customers").cast("int"))
    .withColumn("updated_date", current_timestamp()) # Add trf metadata
)

# Write to silver Delta table
df_silver.write.format("delta") \
    .mode("append") \
    .saveAsTable(silver_table_name)

