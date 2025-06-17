# Databricks notebook source
# MAGIC %pip install pytest pytest-asyncio minio retrying databricks-test azure.servicebus azure.identity

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------

import pytest
import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime

# COMMAND ----------

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace/Repos/feature_vinoth/bh-ccus-data-platform/tests/etl")
print(os.getcwd())


# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main(["test_resample_dts_gold.py", "-vv", "-p", "no:cacheprovider"])

print(retcode)

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

#%pip install pytest-cov

# COMMAND ----------

'''import os
import sys
import pytest

# Change directory to the test directory
os.chdir("/Workspace/Repos/feature_vaishnavi/bh-ccus-data-platform/tests/etl")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem
sys.dont_write_bytecode = True

# Run pytest with coverage
retcode = pytest.main([
    ".", 
    "-vv", 
    "-p", "no:cacheprovider",
    "--cov=/Workspace/Repos/feature_vaishnavi/bh-ccus-data-platform/src", # Path to the source code
    "--cov-report=xml:coverage_xml_report"
    ])

print(retcode)
'''

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

expected_schema = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField(
            "data",
            StructType(
                [
                    StructField("x", FloatType(), True),
                    StructField("y", FloatType(), True),
                    StructField("saturation", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)

# Define the expected data for the result DataFrame
expected_data = [
    ("device_1", {"x": 10.0, "y": 20.0, "saturation": 100}),
    ("device_1", {"x": 15.0, "y": 25.0, "saturation": 200}),
    ("device_2", {"x": 30.0, "y": 40.0, "saturation": 300}),
]

expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
print(expected_df.collect())

# COMMAND ----------

schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("saturation", IntegerType(), True),
        ]
    )

    # Create a sample input DataFrame
input_data = [
    ("device_1", 10.0, 20.0, 100),
    ("device_1", 15.0, 25.0, 200),
    ("device_2", 30.0, 40.0, 300),
]
input_df = spark.createDataFrame(input_data, schema=schema)
print(input_df.collect())