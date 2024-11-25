# Databricks notebook source
storage_account_name = "bnyprojectstorage"
container_name = "market-data"
sas_token = "sp=rcwdl&st=2024-11-25T23:23:49Z&se=2025-02-26T07:23:49Z&spr=https&sv=2022-11-02&sr=c&sig=sc7RJjV2cc7Asvtj%2FluKE%2Fngqz0A1fOi0Nn6y0EnQBU%3D"

# Mount the storage
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point=f"/mnt/{container_name}",
    extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)


# COMMAND ----------

# List all files in the mounted container
display(dbutils.fs.ls("/mnt/market-data"))

# COMMAND ----------

from pyspark.sql.functions import lit

# List of stock symbols
symbols = ["AAPL", "TSLA", "COIN", "WM", "BK", "RIVN", "MNMD"]

# Initialize an empty DataFrame
combined_df = None

# Loop through each stock symbol
for symbol in symbols:
    # Construct file path for each symbol's data
    file_path = f"/mnt/market-data/{symbol}_data.csv"
    
    # Load the .csv file into a temporary DataFrame
    temp_df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Add the `symbol` column to identify the stock
    temp_df = temp_df.withColumn("symbol", lit(symbol))
    
    # Combine with the main DataFrame
    combined_df = temp_df if combined_df is None else combined_df.union(temp_df)

# Reorder columns to make 'symbol' the first column
columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
combined_df = combined_df.select(columns)

# Sort the DataFrame by date and symbol
combined_df = combined_df.orderBy(["date", "symbol"], ascending=[True, True])

# Show a preview of the combined DataFrame
combined_df.show(10, truncate=False)


# COMMAND ----------

# Count rows per symbol and verify distinct stock symbols
combined_df.groupBy("symbol").count().show()




# COMMAND ----------

from pyspark.sql.functions import lag, col, avg
from pyspark.sql.window import Window

# Define a window partitioned by symbol and ordered by date
window_spec = Window.partitionBy("symbol").orderBy("date")

# Add previous day's closing price
combined_df = combined_df.withColumn("prev_close", lag("close").over(window_spec))

# Calculate daily percentage change
combined_df = combined_df.withColumn("daily_change", ((col("close") - col("prev_close")) / col("prev_close")) * 100)

# Add a 7-day moving average for the closing price
combined_df = combined_df.withColumn(
    "7_day_avg",
    avg("close").over(window_spec.rowsBetween(-6, 0))  # A rolling window of 7 days
)

# Show the updated DataFrame with new columns
combined_df.select("symbol", "date", "close", "prev_close", "daily_change", "7_day_avg").show(10, truncate=False)


# COMMAND ----------

output_path = f"/mnt/market-data/processed/enriched_data.parquet"
combined_df.write.parquet(output_path, mode="overwrite")


# COMMAND ----------

display(dbutils.fs.ls("/mnt/market-data/processed/"))
