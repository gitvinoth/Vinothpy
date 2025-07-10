# Databricks notebook source
# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/read_utility

# COMMAND ----------

# MAGIC %run ../utils/databricks/utility_functions

# COMMAND ----------

# MAGIC %run ../utils/databricks/workflow_metadata_utility

# COMMAND ----------

# MAGIC %run ../utils/databricks/constants

# COMMAND ----------

# MAGIC %run ../utils/file_metadata_utility

# COMMAND ----------

import asyncio
import json
import numpy
import os
import time

from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import ClientSecretCredential
from datetime import datetime
from delta.tables import DeltaTable
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    count,
    col,
    array,
    concat_ws,
    max as F_max,
    from_json,
    explode,
    length,
    size
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
    DoubleType,
)

# COMMAND ----------

# for on-prem begin
try:
    if dbutils:
        pass  # pragma: no cover
except NameError:
    from src.utils.logger import configure_logger
    from src.utils.on_premise.utility_functions import (
        get_spark_context,
        get_task_env,
        get_table_name,
        get_job_and_run_id,
        move_file,
    )
    from src.utils.read_utility import read_delta_table
    from src.utils.on_premise.constants import (
        RULES_TABLE_NAME,
        RULES_HEADER_TABLE_NAME,
        COMBINED_RULES_TABLE_NAME,
        ERROR_LOG_TABLE_NAME,
        RULES_BUCKET,
        LOG_FILE_TYPE,
    )

# COMMAND ----------

def get_max_time_from_bronze(spark, logger, catalog, sensor_type):
    """This function returns the maximum timestamp from the bronze zone table for the given sensor type."""
    try:
        max_timestamp = None
        if sensor_type == "pt_gauge":
            max_timestamp = spark.sql(
                f"""select max(epoch_timestamp) as max 
                    from 
                     `{catalog}`.bronze_zone.pt_gauge_electricals
                    union all
                    select unix_timestamp(to_timestamp(time_utc, "MM/dd/yyyy HH:mm:ss")) as timestamp
                    from `{catalog}`.bronze_zone.pt_gauge
                    """
            ).collect()[0]["max"]
        elif sensor_type == "micro_seismic":
            max_timestamp = spark.sql(
                f"select max(epoch_timestamp) as max from `{catalog}`.bronze_zone.microseismic_events"
            ).collect()[0]["max"]
        elif sensor_type == "flowmeter":
            max_timestamp = spark.sql(
                f"""select max(timestamp) as max 
                    from (
                    select unix_timestamp(to_timestamp(time_date, "yyyy-dd-MM HH:mm:ss")) as timestamp
                    from `{catalog}`.bronze_zone.flowmeter_data_as_rows
                    )"""
            ).collect()[0]["max"]
        elif sensor_type == "dss":
            max_timestamp = spark.sql(
                f'SELECT max(`timestamp`) as max from (SELECT unix_timestamp(date_utc,"dd-MMM-yyyy HH:mm:ss") as `timestamp` from `{catalog}`.bronze_zone.dss)'
            ).collect()[0]["max"]
        else:
            logger.info("Unknown sensor type!")
        return max_timestamp
    except Exception as e:
        raise e

# COMMAND ----------

def get_combined_sensors_max_bronze_time(spark, logger, catalog) -> int:
    try:
        query = f"""
        WITH all_sensor_times AS (
            -- PT Gauge timestamps
            SELECT 'pt_gauge' as sensor_type, max(epoch_timestamp) as max_time
            FROM (
                SELECT epoch_timestamp 
                FROM `{catalog}`.bronze_zone.pt_gauge_electricals
                UNION ALL
                SELECT unix_timestamp(to_timestamp(time_utc, "MM/dd/yyyy HH:mm:ss")) as epoch_timestamp
                FROM `{catalog}`.bronze_zone.pt_gauge
            )
            
            UNION ALL
            
            -- Micro seismic timestamps
            SELECT 'micro_seismic' as sensor_type, max(epoch_timestamp) as max_time
            FROM `{catalog}`.bronze_zone.microseismic_events
            
            UNION ALL
            
            -- Flowmeter timestamps
            SELECT 'flowmeter' as sensor_type, max(timestamp) as max_time
            FROM (
                SELECT unix_timestamp(to_timestamp(time_date, "yyyy-dd-MM HH:mm:ss")) as timestamp
                FROM `{catalog}`.bronze_zone.flowmeter_data_as_rows
            )
            
            UNION ALL
            
            -- DSS timestamps
            SELECT 'dss' as sensor_type, max(timestamp) as max_time
            FROM (
                SELECT unix_timestamp(date_utc,"dd-MMM-yyyy HH:mm:ss") as timestamp 
                FROM `{catalog}`.bronze_zone.dss
            )
        )
        SELECT 
            min(max_time) as min_max_time,
            collect_list(struct(sensor_type, max_time)) as sensor_times
        FROM all_sensor_times
        WHERE max_time IS NOT NULL
        """
        result = spark.sql(query).collect()[0]
        return result["min_max_time"]
    except Exception as e:
        # logger.error(f"Error in combined_sensors_max_bronze_time : {str(e)}")
        raise

# COMMAND ----------

def get_combined_sensors_last_run_time(
    spark, logger, catalog: str, run_time_table_name: str, workflow_id: str
) -> int:
    """This function returns the combined last run time for all the sensor types"""
    try:
        # Fetch the last_run_time as min of max of all the sensor types
        try:
            last_run_time = (
                read_delta_table(spark, logger, run_time_table_name)
                .toDF()
                .filter(col("workflow_id") == workflow_id)
                .groupBy(
                    "sensor_type"
                )  # This needs to be bases on combined_rules not sensor types
                .agg(F_max("last_run_time").alias("min_time"))
                .orderBy("min_time")
                .limit(1)
                .collect()[0]["min_time"]
            )
        except IndexError as e:
            last_run_time = 0

        # First time run
        if (last_run_time is None) or (last_run_time == 0):
            logger.info(
                f"Last run time not found in the {run_time_table_name} table... Now fetching from the bronze zone..."
            )
            # Fetch the min of max bronze timestamp from all the sensor types
            last_run_time = get_combined_sensors_max_bronze_time(spark, logger, catalog)
        return last_run_time

    except Exception as e:
        logger.error(f"Error in get_combined_sensors_last_run_time : {str(e)}")
        raise

# COMMAND ----------

def get_last_run_time(
    spark, logger, catalog: str, table_name: str, sensor_type: str, workflow_id: str
) -> int:
    """This function returns the last run time for the given sensor type and workflow id."""
    try:
        # Fetching the last_run_time from the rule_execution_run_time table for subsequent runs after first run
        logger.info(
            f"Fetching last run time for workflow : {workflow_id} and sensor : {sensor_type}..."
        )
        try:
            last_run_time = (
                read_delta_table(spark, logger, table_name)
                .toDF()
                .filter(
                    (col("workflow_id") == workflow_id)
                    & (col("sensor_type") == sensor_type)
                )
                .collect()[0]["last_run_time"]
            )
        except IndexError as e:
            last_run_time = 0

        # Fetching the last time from the bronze table when the workflow runs for the first time
        if (last_run_time == 0) or (last_run_time == None):
            logger.info(
                f"Last run time not found in the {table_name} table... Now fetching from the bronze zone..."
            )
            max_timestamp = get_max_time_from_bronze(
                spark, logger, catalog, sensor_type
            )
            last_run_time = max_timestamp

        return last_run_time
    except Exception as e:
        logger.error(f"Error in get_last_run_time : {str(e)}")
        raise

# COMMAND ----------

def update_last_run_time(
    spark,
    logger,
    table_name: str,
    sensor_type: str,
    workflow_id: str,
    updated_last_run_time: int,
) -> None:
    """This function updates the last run time in the rule_execution_run_time table for the given sensor type and workflow id."""
    try:
        logger.info(
            f"Updating the {table_name} table with updated last run time value {updated_last_run_time} for sensor {sensor_type} and workflow {workflow_id}..."
        )
        rule_execution_run_time_dt = read_delta_table(spark, logger, table_name)

        schema = rule_execution_run_time_dt.toDF().schema

        data = [(workflow_id, sensor_type, updated_last_run_time, datetime.now())]

        rule_execution_run_time_df = spark.createDataFrame(data=data, schema=schema)

        max_retries = 10
        delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                rule_execution_run_time_dt.alias("a").merge(
                    rule_execution_run_time_df.alias("b"),
                    "a.workflow_id = b.workflow_id and a.sensor_type = b.sensor_type",
                ).whenMatchedUpdate(
                    set={
                        "last_run_time": "b.last_run_time",
                        "last_updated_date": "b.last_updated_date",
                    }
                ).whenNotMatchedInsert(
                    values={
                        "workflow_id": "b.workflow_id",
                        "sensor_type": "b.sensor_type",
                        "last_run_time": "b.last_run_time",
                        "last_updated_date": "b.last_updated_date",
                    }
                ).execute()

                break

            except Exception as e:
                if "concurrent update" in str(e):
                    logger.info(
                        f"Concurrent update detected on attempt {attempt}. Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                    delay = delay * 2

                    if attempt == max_retries:
                        logger.error(
                            f"Failed to update {table_name} table with updated last run time value {updated_last_run_time} for sensor {sensor_type} and workflow {workflow_id} after {max_retries} attempts due to concurrent update issues."
                        )
                        raise e

    except Exception as e:
        logger.error(f"Error in update_last_run_time : {str(e)}")
        raise

# COMMAND ----------

def is_empty_bronze(spark, logger, catalog, sensor_type):
    """This function checks the presence of data in bronze zone for the given sensor type. Returns the count of the dataframe."""
    try:
        bronze_count = None
        if sensor_type == "pt_gauge":
            bronze_count = spark.sql(
                f"""select count(*) 
                        from (
                        select count(*)
                        from `{catalog}`.bronze_zone.pt_gauge_electricals
                        union all
                        select count(*)
                        from `{catalog}`.bronze_zone.pt_gauge
                        )"""
            ).collect()[0]["count(1)"]
        elif sensor_type == "micro_seismic":
            bronze_count = spark.sql(
                f"select count(*) from `{catalog}`.bronze_zone.microseismic_events"
            ).collect()[0]["count(1)"]
        elif sensor_type == "flowmeter":
            bronze_count = spark.sql(
                f"select count(*) from `{catalog}`.bronze_zone.flowmeter_data_as_rows"
            ).collect()[0]["count(1)"]
        elif sensor_type == "dss":
            bronze_count = spark.sql(
                f"select count(*) from `{catalog}`.silver_zone.dss"
            ).collect()[0]["count(1)"]

        return bronze_count
    except Exception as e:
        raise e

# COMMAND ----------

class RuleEngine:
    def __init__(
        self,
        spark,
        logger,
        process_name,
        rules_list,
        delay,
        engine_run_frequency,
        topic_name,
        fully_qualified_namespace,
        scope,
        last_run_time,
        catalog,
        downtime_gap_multiplier,
        rules_switch,
    ):
        self.spark = spark
        self.logger = logger
        self.process_name = process_name
        self.rules_list = rules_list
        self.delay = delay
        self.engine_run_frequency = engine_run_frequency
        self.topic_name = topic_name
        self.fully_qualified_namespace = fully_qualified_namespace
        self.scope = scope
        self.last_run_time = last_run_time
        self.catalog = catalog
        self.downtime_gap_multiplier = downtime_gap_multiplier
        self.rules_switch = rules_switch

    async def start(self):
        try:
            self.logger.info(
                f"Process : {self.process_name} | Fetching service bus connection details..."
            )
            AZURE_TENANT_ID = dbutils.secrets.get(self.scope, "AZURE_TENANT_ID")
            AZURE_CLIENT_ID = dbutils.secrets.get(self.scope, "AZURE_CLIENT_ID")
            AZURE_CLIENT_SECRET = dbutils.secrets.get(self.scope, "AZURE_CLIENT_SECRET")

            self.logger.info(
                f"Process : {self.process_name} | Creating Service Bus client..."
            )
            credential = ClientSecretCredential(
                AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
            )
            self.logger.info(
                f"Process : {self.process_name} | Service Bus client created successfully..."
            )
            async with ServiceBusClient(
                self.fully_qualified_namespace,
                credential,
                retry_total=22,
                retry_backoff_factor=5,
                retry_backoff_max=30,
                retry_mode="exponential",
            ) as servicebus_client:
                sender = servicebus_client.get_topic_sender(topic_name=self.topic_name)
                async with sender:
                    self.logger.info(
                        f"Process : {self.process_name} | Starting params creation..."
                    )
                    self.tasks = []
                    # Add a checker for simple_rules v/s combined rules. Based on that set the params
                    for rule in self.rules_list:
                        if self.rules_switch.lower() == "simple_rules":
                            params = {
                                "rule_id": rule["rule_id"],
                                "rule_name": rule["rule_name"],
                                "tenant_id": rule["tenant_id"],
                                "condition_id": rule["condition_id"],
                                "condition_name": rule["condition_name"],
                                "asset_id": rule["asset_id"],
                                "join_condition": rule["join_condition"],
                                "parameter": rule["parameter"],
                                "operator": rule["operator"],
                                "class_val": rule["class"],
                                "threshold": rule["threshold"],
                                "duration": rule["duration"],
                                "wire": rule["wire"],
                                "function": rule["function"],
                                "wire_length_from": rule["wire_length_from"],
                                "wire_length_to": rule["wire_length_to"],
                                "baseline_time": rule["baseline_time"],
                                "threshold_unit": rule["threshold_unit"],
                                "rule_run_frequency": rule["rule_run_frequency"],
                                "sensor_type": rule["sensor_type"],
                                "severity": rule["severity"],
                                "risk_register_controls": rule[
                                    "risk_register_controls"
                                ],
                                "query": rule["query"],
                                "max_data_frequency": rule["max_data_frequency"],
                                "delay": self.delay,
                                "engine_run_frequency": self.engine_run_frequency,
                                "last_run_time": self.last_run_time,
                                "sender": sender,
                                "catalog": self.catalog,
                                "downtime_gap_multiplier": self.downtime_gap_multiplier,
                            }
                        elif self.rules_switch.lower() == "combined_rules":
                            params = {
                                "rule_id": rule["rule_id"],
                                "rule_name": rule["rule_name"],
                                "tenant_id": rule["tenant_id"],
                                "severity": rule["severity"],
                                "join_condition": rule["join_condition"],
                                "risk_register_controls": rule[
                                    "risk_register_controls"
                                ],
                                "condition_group_id": rule["condition_group_id"],
                                "conditions": rule["conditions"],
                                "query": rule["query"],
                                "delay": self.delay,
                                "engine_run_frequency": self.engine_run_frequency,
                                "last_run_time": self.last_run_time,
                                "sender": sender,
                                "catalog": self.catalog,
                                "downtime_gap_multiplier": self.downtime_gap_multiplier,
                            }
                        self.tasks.append(
                            asyncio.create_task(
                                self.execute_rule(
                                    self.spark, self.logger, params, self.rules_switch
                                )
                            )
                        )
                    await asyncio.wait(self.tasks)
                await credential.close()
        except Exception as e:
            self.logger.error(
                f"Process : {self.process_name} | Error in start : {str(e)}"
            )
            raise

    def get_start_and_end_time(
        self,
        logger,
        rule_run_frequency: int,
        delay: int,
        engine_run_frequency: int,
        run_time: int,
        downtime_gap_multiplier: int,
    ):
        try:
            self.logger.info(
                f"Process : {self.process_name} | Computing Start time and End time..."
            )
            start_time_str = None
            end_time_str = None

            current_time = run_time - delay
            mod = current_time % rule_run_frequency

            if rule_run_frequency <= engine_run_frequency:
                end_time = (
                    current_time
                    - mod
                    + (engine_run_frequency * downtime_gap_multiplier)
                )
                end_time_str = str(int(end_time))
                start_time = (
                    current_time
                    - engine_run_frequency
                    - (current_time - engine_run_frequency) % rule_run_frequency
                )
                start_time_str = str(int(start_time))
            elif mod < engine_run_frequency:
                end_time = (
                    current_time
                    - mod
                    + (engine_run_frequency * downtime_gap_multiplier)
                )
                end_time_str = str(int(end_time))
                start_time = (
                    end_time
                    - rule_run_frequency
                    - (engine_run_frequency * downtime_gap_multiplier)
                )
                start_time_str = str(int(start_time))
            return start_time_str, end_time_str
        except Exception as e:
            self.logger.error(
                f"Process : {self.process_name} | Error in get_start_and_end_time : {str(e)}"
            )
            raise

    async def execute_rule(
        self,
        spark,
        logger,
        params: dict,
        rules_switch: str,
    ) -> None:
        try:
            # Setting the params for simple_rules
            if rules_switch.lower() == "simple_rules":
                rule_id = params.get("rule_id")
                rule_name = params.get("rule_name")
                tenant_id = params.get("tenant_id")
                condition_id = params.get("condition_id")
                condition_name = params.get("condition_name")
                asset_id = params.get("asset_id")
                join_condition = params.get("join_condition")
                parameter = params.get("parameter")
                operator = params.get("operator")
                class_val = params.get("class_val")
                threshold = params.get("threshold")
                duration = params.get("duration")
                wire = params.get("wire")
                function = params.get("function")
                wire_length_from = params.get("wire_length_from")
                wire_length_to = params.get("wire_length_to")
                baseline_time = params.get("baseline_time")
                threshold_unit = params.get("threshold_unit")
                rule_run_frequency = params.get("rule_run_frequency")
                sensor_type = params.get("sensor_type")
                severity = params.get("severity")
                risk_register_controls = params.get("risk_register_controls")
                query = params.get("query")
                max_data_frequency = params.get("max_data_frequency")
                delay = params.get("delay")
                engine_run_frequency = params.get("engine_run_frequency")
                last_run_time = params.get("last_run_time")
                sender = params.get("sender")
                catalog = params.get("catalog")
                downtime_gap_multiplier = params.get("downtime_gap_multiplier")

            # Setting the params for combined rules
            elif rules_switch.lower() == "combined_rules":
                rule_id = params.get("rule_id")
                rule_name = params.get("rule_name")
                tenant_id = params.get("tenant_id")
                severity = params.get("severity")
                join_condition = params.get("join_condition")
                risk_register_controls = params.get("risk_register_controls")
                condition_group_id = params.get("condition_group_id")
                conditions = params.get("conditions")
                rule_run_frequency = 1
                delay = params.get("delay")
                engine_run_frequency = params.get("engine_run_frequency")
                last_run_time = params.get("last_run_time")
                sender = params.get("sender")
                catalog = params.get("catalog")
                downtime_gap_multiplier = params.get("downtime_gap_multiplier")
                max_data_frequency = 1
                query = params.get("query")

            # Calling function and obtaining start and end time for each row of Rules table
            start_time_str, end_time_str = self.get_start_and_end_time(
                self.logger,
                rule_run_frequency,
                delay,
                engine_run_frequency,
                last_run_time,
                downtime_gap_multiplier,
            )

            self.logger.info(
                f"Process : {self.process_name} | Rule id : {rule_id} | Executing for rule_id : {rule_id}, "
            )

            # Replacing the variable in the query with the values for a valid start_time and end_time
            if (start_time_str is not None) and (end_time_str is not None):
                formatted_query = (
                    query.replace("$catalog", catalog)
                    .replace("$start_time", start_time_str)
                    .replace("$end_time", end_time_str)
                )
                self.logger.info(
                    f"Process : {self.process_name} | Rule id : {rule_id} | start_time_str : {start_time_str}, end_time_str : {end_time_str}, data_frequency : {max_data_frequency}, "
                )

                results = spark.sql(formatted_query).cache()
                self.logger.info("Query executed ...")

                # Checking if any anomaly is found. If no anomaly is found, nothing will be sent to service bus----------
                if results.count() > 0:
                    anomalies = []

                    # Creating the Anomalies list
                    for result_row in results.collect():
                        result_dict = result_row.asDict()
                        anomalies.append(result_dict)

                    # Creating the time range dictionary
                    time_range_dict = {
                        "start": int(start_time_str) - (max_data_frequency * 2 - 1),
                        "end": int(end_time_str),
                    }

                    # Creating the conditions array for simple_rules
                    if rules_switch.lower() == "simple_rules":
                        self.logger.info(f"Creating conditions json for simple rules")
                        conditions_arr = [
                            {
                                "condition_id": condition_id,
                                "condition_name": condition_name,
                                "asset_id": asset_id,
                                "parameter": parameter,
                                "operator": operator,
                                "class": class_val,
                                "threshold": threshold,
                                "duration": duration,
                                "rule_run_frequency": rule_run_frequency,
                                "sensor_type": sensor_type,
                                "wire_length_from": wire_length_from,
                                "wire_length_to": wire_length_to,
                                "function": function,
                                "wire": wire,
                                "baseline_time": baseline_time,
                                "threshold_unit": threshold_unit,
                            }
                        ]
                    elif rules_switch.lower() == "combined_rules":
                        self.logger.info(f"Creating conditions json for combined rules")
                        conditions_arr = conditions
                    else:
                        self.logger.info(f"Invalid rules_switch : {rules_switch}")

                    # Creating the rules dictionary consisting of all the required information for anomaly payload
                    rule_dict = {
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "tenant_id": tenant_id,
                        "severity": severity,
                        "join_condition": join_condition,
                        "condition_group_id": 0
                        if self.rules_switch == "simple_rules"
                        else condition_group_id,
                        "risk_register_controls": risk_register_controls,
                        "conditions": conditions_arr,
                    }

                    self.logger.info(
                        f"Process : {self.process_name} | Rule id : {rule_id} | Preparing the message..."
                    )

                    # Creating the message object for sending to service bus
                    message = {
                        "anomalies": anomalies,
                        "time_range": time_range_dict,
                        "rule": rule_dict,
                    }

                    self.logger.info(
                        f"Process : {self.process_name} | Rule id : {rule_id} | Sending the message..."
                    )
                    await sender.send_messages(
                        ServiceBusMessage(str(json.dumps(message)))
                    )
                    self.logger.info(
                        f"Process : {self.process_name} | Rule id : {rule_id} | {json.dumps(message)} message sent successfully..."
                    )
                else:
                    self.logger.info(
                        f"Process : {self.process_name} | Rule id : {rule_id} | No anomaly found for rule_id : {rule_id}"
                    )
            else:
                self.logger.info(
                    f"Process : {self.process_name} | Rule id : {rule_id} | Rule not executed"
                )
            self.logger.info(
                f"Process : {self.process_name} | Rule id : {rule_id} | Completed successfully"
            )
        except Exception as e:
            self.logger.error(
                f"Error in execute_rule :  Process : {self.process_name} | Rule id : {rule_id} | Encountered error :  {str(e)}"
            )
            raise

# COMMAND ----------

def row_to_dict(row):
    """Convert a Row object to dictionary"""
    if isinstance(row, list):
        return [row_to_dict(item) for item in row]
    elif hasattr(row, "__fields__"):  # Check if it's a Row object
        return {
            "condition_id": row.condition_id,
            "condition_name": row.condition_name,
            "asset_id": row.asset_id,
            "parameter": row.parameter,
            "operator": row.operator,
            "threshold": row.threshold,
            "duration": row.duration,
            "wire_length_from": row.wire_length_from,
            "wire_length_to": row.wire_length_to,
            "rule_run_frequency": row.rule_run_frequency,
            "sensor_type": row.sensor_type,
            "threshold_unit": row.threshold_unit,
            "baseline_time": row.baseline_time,
            "function": row["function"],
            "wire": row.wire,
            "class": row["class"],
        }
    else:
        return row


def process_row_dict(row_dict):
    """Process the row dictionary to convert any Row objects in conditions"""
    if "conditions" in row_dict:
        row_dict["conditions"] = row_to_dict(row_dict["conditions"])
    return row_dict


def get_rules_list(
    logger, df: DataFrame, no_of_chunks: int, config_params: dict, rules_switch: str
) -> list:
    """This function returns a rules list from the given dataframe"""
    try:
        # Processing simple rules into rules_list
        if rules_switch.lower() == "simple_rules":
            logger.info(
                "Generating the simple_rules list by applying transformations..."
            )
            row_list = []
            for row in df.collect():
                row_list.append(row.asDict())
            array = numpy.array_split(row_list, no_of_chunks)

            rules_list = []
            for element in array:
                rules_list.append((element, config_params))
            return rules_list

        # Processing combined rules into rules_list
        elif rules_switch.lower() == "combined_rules":
            logger.info(
                "Generating the combined_rules list by applying transformations..."
            )
            row_list = []
            for row in df.collect():
                row_dict = row.asDict()
                processed_row = process_row_dict(row_dict)
                row_list.append(processed_row)
            array = numpy.array_split(row_list, no_of_chunks)

            rules_list = []
            for element in array:
                rules_list.append((element, config_params))
            return rules_list

        # Handling cases other than simple_rules and combined_rules
        else:
            logger.info(
                f"Invalid switch value: {rules_switch}. Hence rules_list generation skipped..."
            )

    except Exception as e:
        logger.error(f"Error in get_rules_list : {str(e)}")
        raise

# COMMAND ----------

def run(spark, logger, rules_list: list, config_params: dict) -> None:
    try:
        process_name = Process().name
        logger.info(f"Running {process_name}...")
        logger.info(f"Length of rules list: {len(rules_list)}")
        if (len(rules_list) == 0) or (rules_list is None):
            logger.info(f"Process : {process_name} | No Rules to execute...")
        else:
            delay = config_params["delay"]
            engine_run_frequency = config_params["engine_run_frequency"]
            topic_name = config_params["topic_name"]
            fully_qualified_namespace = config_params["fully_qualified_namespace"]
            scope = config_params["scope"]
            last_run_time = config_params["last_run_time"]
            catalog = config_params["catalog"]
            downtime_gap_multiplier = config_params["downtime_gap_multiplier"]
            rules_switch = config_params["rules_switch"]

            # Creating the RuleEngine object by initializing the object parameters
            logger.info("Creating the rule object")
            rule_execution = RuleEngine(
                spark,
                logger,
                process_name,
                rules_list,
                delay,
                engine_run_frequency,
                # data_frequency,
                topic_name,
                fully_qualified_namespace,
                scope,
                last_run_time,
                catalog,
                downtime_gap_multiplier,
                rules_switch,
            )

            # Running the rules by calling the object.start() method
            asyncio.run(rule_execution.start())
    except Exception as e:
        logger.error(f"Error in run : {str(e)}")
        raise

# COMMAND ----------

def get_data_differential(max_bronze_time_value, current_time_value):
    try:
        if (max_bronze_time_value is not None) and (max_bronze_time_value) > 0:
            return current_time_value - max_bronze_time_value
        return 0
    except Exception as e:
        logger.error(f"Error in get_data_differential : {str(e)}")
        raise


def get_downtime_gap_multiplier(
    current_time_value,
    data_differential_value,
    last_run_time_value,
    engine_run_frequency_value,
):
    try:
        if (current_time_value - data_differential_value) > last_run_time_value:
            return (
                current_time_value - data_differential_value - last_run_time_value
            ) / engine_run_frequency_value
        else:
            return 0
    except Exception as e:
        logger.error(f"Error in get_downtime_gap_multiplier : {str(e)}")
        raise

# COMMAND ----------

def get_rule_engine_config_params(
    spark,
    logger,
    catalog: str,
    sensor_type: str,
    delay: int,
    engine_run_frequency: int,
    topic_name: str,
    fully_qualified_namespace: str,
    scope: str,
    run_time_table_name: str,
    workflow_id: str,
    downtime_seconds: int,
    last_run_time: int,
    max_bronze_time_value: int,
    current_time_value: int,
) -> dict:
    try:
        data_differential = get_data_differential(
            max_bronze_time_value, current_time_value
        )
        logger.info(f"Data differential is : {data_differential}")

        if last_run_time < current_time_value - data_differential - downtime_seconds:
            last_run_time = current_time_value - data_differential - downtime_seconds
        downtime_gap_multiplier = get_downtime_gap_multiplier(
            current_time_value, data_differential, last_run_time, engine_run_frequency
        )
        logger.info(f"Downtime gap multiplier is : {downtime_gap_multiplier}")
        config_params = {
            "delay": delay,
            "engine_run_frequency": engine_run_frequency,
            "topic_name": topic_name,
            "fully_qualified_namespace": fully_qualified_namespace,
            "scope": scope,
            "last_run_time": last_run_time,
            "downtime_gap_multiplier": downtime_gap_multiplier,
            "catalog": catalog,
        }
        return config_params, downtime_gap_multiplier
    except Exception as e:
        logger.error(f"Error in get_rule_engine_config_params : {str(e)}")
        raise

# COMMAND ----------

def get_combined_rules_transformed_df(
    spark, logger, rules_df, rules_header_df
) -> DataFrame:
    try:
        if (rules_header_df is None) or (rules_header_df.count() == 0):
            logger.info("No header data. Hence skipping the rule execution...")
            return
        else:
            condition_schema = StructType(
                [
                    StructField("condition_id", IntegerType(), True),
                    StructField("condition_name", StringType(), True),
                    StructField("asset_id", ArrayType(StringType()), True),
                    StructField("parameter", StringType(), True),
                    StructField("operator", StringType(), True),
                    StructField("threshold", DoubleType(), True),
                    StructField("duration", IntegerType(), True),
                    StructField("rule_run_frequency", IntegerType(), True),
                    StructField("wire_length_from", IntegerType(), True),
                    StructField("wire_length_to", IntegerType(), True),
                    StructField("sensor_type", StringType(), True),
                    StructField("threshold_unit", StringType(), True),
                    StructField("baseline_time", IntegerType(), True),
                    StructField("function", StringType(), True),
                    StructField("wire", StringType(), True),
                    StructField("class", IntegerType(), True),
                ]
            )
            combined_rules_df = (
                rules_df.alias("a")
                .join(
                    rules_header_df.alias("b"),
                    rules_df["rule_id"] == rules_header_df["rule_id"],
                    "inner",
                )
                .select(
                    col("b.rule_id").alias("rule_id"),
                    col("b.rule_name").alias("rule_name"),
                    col("b.tenant_id").alias("tenant_id"),
                    col("b.severity").alias("severity"),
                    col("b.join_condition").alias("join_condition"),
                    col("b.condition_group_id").alias("condition_group_id"),
                    col("b.risk_register_controls").alias("risk_register_controls"),
                    from_json(col("b.conditions"), ArrayType(condition_schema)).alias(
                        "conditions"
                    ),
                    col("a.combined_query").alias("query"),
                )
            )

            final_combined_df = combined_rules_df.select(
                "rule_id",
                "rule_name",
                "tenant_id",
                "severity",
                "join_condition",
                "condition_group_id",
                "risk_register_controls",
                "conditions",
                "query",
            )
            return final_combined_df
    except Exception as e:
        logger.error(f"Error in get_combined_rules_transformed_df : {str(e)}")
        raise

# COMMAND ----------

def handle_simple_rules(
    spark,
    logger,
    catalog: str,
    table_name: str,
    sensor_type: str,
    delay: int,
    engine_run_frequency: int,
    topic_name: str,
    fully_qualified_namespace: str,
    scope: str,
    run_time_table_name: str,
    workflow_id: str,
    downtime_seconds: int,
    switch: str,
) -> None:
    try:
        rules_df = read_delta_table(spark, logger, table_name).toDF()

        # Filtering data on sensor_type[passed from Workflow task]
        if sensor_type == "pt_gauge":
            rules_filtered_df = rules_df.filter(
                (col("parameter") == "pressure") | (col("parameter") == "temperature")
            )
        elif sensor_type == "micro_seismic":
            rules_filtered_df = rules_df.filter(
                (col("parameter") == "no_of_events") | (col("parameter") == "magnitude")
            )
        elif sensor_type == "flowmeter":
            rules_filtered_df = rules_df.filter(
                (col("parameter") == "surface_flow_rate")
                | (col("parameter") == "well_head_pressure")
            )
        elif sensor_type == "dss":
            rules_filtered_df = rules_df.filter(
                (col("parameter") == "dts")
                | (col("parameter") == "axial_strain")
                | (col("parameter") == "bend_magnitude")
            )

        if (
            rules_filtered_df.count() > 0
            and is_empty_bronze(spark, logger, catalog, sensor_type) > 0
        ):
            last_run_time = get_last_run_time(
                spark, logger, catalog, run_time_table_name, sensor_type, workflow_id
            )
            current_time = int(datetime.now().timestamp())
            logger.info(f"Current time is : {current_time}")
            max_bronze_time = get_max_time_from_bronze(
                spark, logger, catalog, sensor_type
            )
            logger.info(f"Max bronze time is : {max_bronze_time}")
            (
                config_params,
                downtime_gap_multiplier,
            ) = get_rule_engine_config_params(
                spark,
                logger,
                catalog,
                sensor_type,
                delay,
                engine_run_frequency,
                topic_name,
                fully_qualified_namespace,
                scope,
                run_time_table_name,
                workflow_id,
                downtime_seconds,
                last_run_time,
                max_bronze_time,
                current_time,
            )

            # Adding the rules_switch into the config params for unpacking
            config_params["rules_switch"] = switch
            no_of_cores = 2 * os.cpu_count()

            # Filter the rules_df to fetch only simple_queries
            filtered_combined_query_df = (
                rules_filtered_df.groupBy(col("rule_id"))
                .agg(count("condition_id").alias("condition_count"))
                .filter(col("condition_count") == 1)
                .collect()
            )
            filtered_combined_query_dicts = [
                row.asDict() for row in filtered_combined_query_df
            ]
            rule_id_list = [d["rule_id"] for d in filtered_combined_query_dicts]
            simple_rules_df_list = rules_filtered_df.filter(
                col("rule_id").isin(rule_id_list)
            )

            # Passing the simple_rules_list to the handler
            rules_list = get_rules_list(
                logger, simple_rules_df_list, no_of_cores, config_params, switch
            )
            # Add spark and logger to each tuple in the rules_list
            new_rules_list = [(spark, logger) + rule_tuple for rule_tuple in rules_list]

            logger.info(f"Starting Threadpool for no_of_cores : {no_of_cores}")

            with ThreadPool(no_of_cores) as pool:
                pool.starmap(run, new_rules_list)

            # After Rule exection completion
            if downtime_gap_multiplier > 0:
                last_run_time = int(
                    last_run_time + engine_run_frequency * downtime_gap_multiplier
                )
            updated_last_run_time = int(last_run_time + engine_run_frequency)
            update_last_run_time(
                spark,
                logger,
                run_time_table_name,
                sensor_type,
                workflow_id,
                updated_last_run_time,
            )

        else:
            logger.info(f"No rules to execute for sensor_type : {sensor_type}")
    except Exception as e:
        logger.error(f"Error in handle_simple_rules: {str(e)}")
        raise

# COMMAND ----------

def handle_complex_rules(
    spark,
    logger,
    catalog: str,
    table_name: str,
    sensor_type: str,
    delay: int,
    engine_run_frequency: int,
    topic_name: str,
    fully_qualified_namespace: str,
    scope: str,
    run_time_table_name: str,
    rule_header_table_name: str,
    workflow_id: str,
    downtime_seconds: int,
    switch: str,
) -> None:
    try:
        rules_df = read_delta_table(spark, logger, table_name).toDF()
        rules_header_df = read_delta_table(spark, logger, rule_header_table_name).toDF()
        if (rules_df is not None) and (rules_df.count() > 0):
            last_run_time = get_combined_sensors_last_run_time(
                spark, logger, catalog, run_time_table_name, workflow_id
            )
            current_time = int(datetime.now().timestamp())
            logger.info(f"Current time is : {current_time}")
            max_bronze_time = get_combined_sensors_max_bronze_time(
                spark, logger, catalog
            )
            logger.info(f"Combined min(max_bronze_time) is : {max_bronze_time}")

            (config_params, downtime_gap_multiplier) = get_rule_engine_config_params(
                spark,
                logger,
                catalog,
                sensor_type,
                delay,
                engine_run_frequency,
                topic_name,
                fully_qualified_namespace,
                scope,
                run_time_table_name,
                workflow_id,
                downtime_seconds,
                last_run_time,
                max_bronze_time,
                current_time,
            )

            # Adding the rules_switch into the config params for unpacking
            config_params["rules_switch"] = switch
            no_of_cores = 2 * os.cpu_count()

            # Filter the rules_df to fetch only combined_queries
            filtered_combined_query_df = rules_df.filter(size(col("condition_id")) > 1)
            combined_rules_transformed_df = get_combined_rules_transformed_df(
                spark, logger, filtered_combined_query_df, rules_header_df
            )
            # The rules_df needs to be joined with the input_json_df and pass the query params
            rules_list = get_rules_list(
                logger,
                combined_rules_transformed_df,
                no_of_cores,
                config_params,
                switch,
            )

            # # Add spark and logger to each tuple in the rules_list
            new_rules_list = [(spark, logger) + rule_tuple for rule_tuple in rules_list]

            logger.info(f"Starting Threadpool for no_of_cores : {no_of_cores}")

            with ThreadPool(no_of_cores) as pool:
                pool.starmap(run, new_rules_list)

            # # After Rule exection completion
            if downtime_gap_multiplier > 0:
                last_run_time = int(
                    last_run_time + engine_run_frequency * downtime_gap_multiplier
                )
            updated_last_run_time = int(last_run_time + engine_run_frequency)
            update_last_run_time(
                spark,
                logger,
                run_time_table_name,
                sensor_type,
                workflow_id,
                updated_last_run_time,
            )
        else:
            logger.info(f"No rules to execute for sensor_type : {sensor_type}")
    except Exception as e:
        logger.error(f"Error in handle_complex_rules: {str(e)}")
        raise

# COMMAND ----------

def execute(
   spark,
    logger,
    catalog: str,
    table_name: str,
    combined_rules_table_name:str,
    rules_header_table_name: str,
    sensor_type: str,
    delay: int,
    engine_run_frequency: int,
    topic_name: str,
    fully_qualified_namespace: str,
    scope: str,
    run_time_table_name: str,
    workflow_id: str,
    downtime_seconds: int,
    switch,
) -> None:
    try:
        if switch.lower() == "simple_rules":
            handle_simple_rules(
                spark,
                logger,
                catalog,
                table_name,
                sensor_type,
                delay,
                engine_run_frequency,
                topic_name,
                fully_qualified_namespace,
                scope,
                run_time_table_name,
                workflow_id,
                downtime_seconds,
                switch,
            )
        elif switch.lower() == "combined_rules":
            handle_complex_rules(
                spark,
                logger,
                catalog,
                combined_rules_table_name,
                sensor_type,
                delay,
                engine_run_frequency,
                topic_name,
                fully_qualified_namespace,
                scope,
                run_time_table_name,
                rules_header_table_name,
                workflow_id,
                downtime_seconds,
                switch,
            )
        else:
            raise Exception("Invalid switch value")
    except Exception as e:
        logger.error(f"Error in rule_engine : {str(e)}")
        raise

# COMMAND ----------

def main():
    try:
        # Job Parameters
        sensor_type = get_task_env("sensor_type")
        topic_name = get_task_env("topic_name")
        delay = int(get_task_env("delay"))
        engine_run_frequency = int(get_task_env("engine_run_frequency"))
        downtime_seconds = int(get_task_env("downtime_seconds"))
        scope = get_task_env("scope")
        switch = get_task_env("switch")

        job_start_timestamp = datetime.now()
        app_name = f"{sensor_type}_rule_execution"
        date = job_start_timestamp.strftime("%Y-%m-%d-%H-%M-%S-%f")
        logger = configure_logger(app_name, date)

        # Environment Variables
        logger.info("Fetch values from catalog & storage_host environment variables...")
        servicebus_namespace = os.getenv("servicebus_namespace")
        catalog = os.getenv("catalog")
        storage_host = os.getenv("storage_host")

        logger.info("Fetch job id, run id and task id...")
        workflow_id, run_id, task_id = get_job_and_run_id()
        logger.info(
            f"Value of Job ID: {workflow_id} | Run ID: {run_id} | Task ID: {task_id}"
        )

        spark = get_spark_context()

        fully_qualified_namespace = f"{servicebus_namespace}.servicebus.windows.net"
        rules_table_name = get_table_name(catalog, "bronze_zone", RULES_TABLE_NAME)
        combined_rules_table_name = get_table_name(catalog,"bronze_zone",COMBINED_RULES_TABLE_NAME)
        rules_header_table_name = get_table_name(catalog, "bronze_zone",RULES_HEADER_TABLE_NAME)
        run_time_table_name = get_table_name(
            catalog, "bronze_zone", "rule_execution_run_time"
        )

        logger.info(f"Starting {app_name} | workflow id : {workflow_id}")

        execute(
            spark,
            logger,
            catalog,
            rules_table_name,
            combined_rules_table_name,
            rules_header_table_name,
            sensor_type,
            delay,
            engine_run_frequency,
            topic_name,
            fully_qualified_namespace,
            scope,
            run_time_table_name,
            workflow_id,
            downtime_seconds,
            switch,
        )
    except Exception as e:
        raise
    finally:
        move_file(
            f"file:/tmp/{app_name}_{date}.log",
            f"{storage_host}/logs/{app_name}/{app_name}_{date}.log",
            LOG_FILE_TYPE,
        )

# COMMAND ----------

if __name__ == "__main__":
    main()  # pragma no cover


# COMMAND ----------

# External Rule Execution Support
import types

def run_external_rule_script(rule_path: str, parameters_data, current_time_range, rule, logger):
    logger.info(f"Loading rule from: {rule_path}")

    # Read file from ADLS (via dbutils)
    script_text = dbutils.fs.head(rule_path, 100000)  # Adjust byte size if needed

    # Dynamically execute in a temporary module
    rule_module = types.ModuleType("external_rule")
    exec(script_text, rule_module.__dict__)

    if hasattr(rule_module, "__main"):
        rule_module.__main(parameters_data, current_time_range, rule)
    else:
        logger.error(f"Rule file {rule_path} does not contain __main()")
