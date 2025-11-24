import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sum as _sum, avg as _avg,
    round
)

# Load config
config_path = "/Workspace/Repos/kohjoseph97@gmail.com/Spark-ETL-Pipeline-Demo/etl_demo/config/paths.yaml"
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

silver_table_name = config['silver_table_name']
gold_table_name   = config['gold_table_name']
catalog_name      = config['catalog_name']

# Spark session
spark = SparkSession.builder.appName("SilverToGoldTransformation").getOrCreate()

# Use correct catalog
spark.sql(f"USE CATALOG `{catalog_name}`")

# Read silver table
df_silver = spark.table(silver_table_name)


# Aggregate & compute business KPIs by date

df_gold = (
    df_silver
    .groupBy("date")
    .agg(
        _sum("revenue").alias("total_revenue"),
        _sum("revenue_goal").alias("total_revenue_goal"),
        _sum("margin").alias("total_margin"),
        _sum("margin_goal").alias("total_margin_goal"),
        _sum("sales_quantity").alias("total_sales_quantity"),
        _sum("customers").alias("total_customers"),
        _avg("margin").alias("avg_margin"),
        _avg("revenue").alias("avg_revenue")
    )
    # KPI calculations
    .withColumn("revenue_goal_attainment",
                round(col("total_revenue") / col("total_revenue_goal"), 4))
    .withColumn("margin_goal_attainment",
                round(col("total_margin") / col("total_margin_goal"), 4))
    # Metadata
    .withColumn("processed_timestamp", current_timestamp())
)

# Write to gold Delta table
df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(gold_table_name)
