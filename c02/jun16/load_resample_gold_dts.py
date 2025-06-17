from delta import DeltaTable
from itertools import product
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.functions import (
    from_unixtime,
    col,
    date_format,
    date_trunc,
    first,
    to_timestamp,
    unix_timestamp,
    window,
)

# COMMAND ----------

try:
    if dbutils:
        pass # pragma: no cover
except NameError:
    from src.utils.write_utility import write_table
    from src.utils.read_utility import read_delta_table

# COMMAND ----------

# DBTITLE 1,Archive
def generate_resample_dts_query_by_bucket(
   logger,
   source_table_name: str,
   partition_cols: str,  # Must include "asset_id, depth"
   bucket_size_seconds: int,
) -> str:
   """
   Generates a SQL query that buckets DTS sensor data based on time intervals
   (e.g., 60s, 120s, 3600s...) and computes temperature_sum and temperature_count
   per (asset_id, depth, time bucket).
   """
   try:
       query = f"""
       WITH raw AS (
           SELECT
               asset_id,
               depth,
               FLOOR(timestamp / {bucket_size_seconds}) * {bucket_size_seconds} AS timestamp,
               temperature
           FROM {source_table_name}
           WHERE asset_id IS NOT NULL AND depth IS NOT NULL AND temperature IS NOT NULL
       ),
       agg AS (
           SELECT
               asset_id,
               depth,
               timestamp,
               SUM(temperature) AS temperature_sum,
               COUNT(temperature) AS temperature_count
           FROM raw
           GROUP BY asset_id, depth, timestamp
       ),
       unioned AS (
           SELECT asset_id, 'temperature_sum' AS parameter, timestamp, depth, temperature_sum AS value FROM agg
           UNION ALL
           SELECT asset_id, 'temperature_count' AS parameter, timestamp, depth, temperature_count AS value FROM agg
       )
       SELECT DISTINCT *
       FROM unioned
       ORDER BY asset_id, depth, timestamp
       """
       logger.info(f"[generate_resample_dts_query_by_bucket] Bucket: {bucket_size_seconds} sec, Query: {query}")
       return query
   except Exception as e:
       logger.error(f"Error in generate_resample_dts_query_by_bucket: {str(e)}")
       raise

# COMMAND ----------

def generate_resample_dts_query_by_bucket(
   logger,
   source_table_name: str,
   bucket_size_seconds: int,
) -> str:
   """
   Generates a SQL query that buckets DTS sensor data based on time intervals
   and computes: sum, count, min, max — and includes min/max timestamps.
   """
   try:
       query = f"""
       WITH raw AS (
           SELECT
               asset_id,
               depth,
               timestamp,
               FLOOR(timestamp / {bucket_size_seconds}) * {bucket_size_seconds} AS bucket_start,
               temperature
           FROM {source_table_name}
           WHERE asset_id IS NOT NULL AND depth IS NOT NULL AND temperature IS NOT NULL
       ),
       agg AS (
           SELECT
               asset_id,
               depth,
               bucket_start AS timestamp,
               SUM(temperature) AS temperature_sum,
               COUNT(temperature) AS temperature_count,
               MIN(temperature) AS temperature_min,
               MAX(temperature) AS temperature_max,
               MIN_BY(timestamp, temperature) AS temperature_min_ts,
               MAX_BY(timestamp, temperature) AS temperature_max_ts
           FROM raw
           GROUP BY asset_id, depth, bucket_start
       ),
       unioned AS (
           SELECT asset_id, 'temperature_sum' AS parameter, timestamp, depth, temperature_sum AS value FROM agg
           UNION ALL
           SELECT asset_id, 'temperature_count' AS parameter, timestamp, depth, temperature_count AS value FROM agg
           UNION ALL
           SELECT asset_id, 'temperature_min' AS parameter, temperature_min_ts AS timestamp, depth, temperature_min AS value FROM agg
           UNION ALL
           SELECT asset_id, 'temperature_max' AS parameter, temperature_max_ts AS timestamp, depth, temperature_max AS value FROM agg
       )
       SELECT DISTINCT *
       FROM unioned
       ORDER BY asset_id, depth, timestamp
       """
       logger.info(f"[generate_resample_dts_query_by_bucket] Query with min/max: {query}")
       return query
   except Exception as e:
       logger.error(f"Error generating DTS resample query: {str(e)}")
       raise

# COMMAND ----------

def resample_gold_dts(
   spark,
   logger,
   source_table_name: str,
   partition_cols: str,
   destination_table_name: str,
   bucket_size_seconds: int
) -> bool:
   """
   Executes DTS resampling by aggregating into fixed-size time buckets
   (e.g., 60s, 120s, 3600s, etc.) and writes the result into a gold Delta table.
   Generates one record for each parameter (temperature_sum and temperature_count)
   per (asset_id, depth, bucket timestamp).
   """
   try:
       query = generate_resample_dts_query_by_bucket(
           logger=logger,
           source_table_name=source_table_name,
           bucket_size_seconds=bucket_size_seconds
       )
       if query:
           resampled_df = spark.sql(query)
           dts_gold_table = read_delta_table(spark, logger, destination_table_name)
           (
               dts_gold_table.alias("target")
               .merge(
                   resampled_df.alias("source"),
                   """
                   target.asset_id = source.asset_id AND
                   target.parameter = source.parameter AND
                   target.timestamp = source.timestamp AND
                   target.depth = source.depth
                   """
               )
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute()
           )
           logger.info(f"[resample_gold_dts] Merge complete → {destination_table_name}")
           return True
       else:
           logger.warning("[resample_gold_dts] Empty query generated.")
           return False
   except Exception as e:
       logger.error(f"Error in resample_gold_dts: {str(e)}")
       raise