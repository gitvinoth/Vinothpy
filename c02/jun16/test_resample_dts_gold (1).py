import pytest
from unittest.mock import Mock, patch, call
from src.etl.load_resample_gold_dts_test import generate_resample_dts_query_by_bucket, resample_gold_dts

def test_generate_resample_dts_query_by_bucket_creates_expected_query():
    logger = Mock()
    table = "db.schema.table"
    bucket = 3600

    sql = generate_resample_dts_query_by_bucket(logger, table, bucket)
    assert "FLOOR(timestamp / 3600) * 3600" in sql
    assert table in sql
    assert "temperature_sum" in sql
    logger.info.assert_called_once()
    # Check min/max sections
    assert "temperature_min" in sql
    assert "temperature_max" in sql
    assert "MIN_BY" in sql
    assert "MAX_BY" in sql

def test_generate_resample_dts_query_by_bucket_handles_error():
    logger = Mock()
    # Bad input: non-integer bucket size string
    with pytest.raises(Exception):
        generate_resample_dts_query_by_bucket(logger, "table", "not_an_integer")
    logger.error.assert_called()


from unittest.mock import patch, MagicMock, mock_open
from pyspark.sql import SparkSession, DataFrame, Row


@patch("load_resample_gold_dts_test.read_delta_table")
def test_resample_gold_dts_happy_path(mock_read_delta_table):
    # Setup
    spark = Mock()
    logger = Mock()
    source_table = "my_source"
    dest_table = "my_dest"
    partition_cols = "asset_id,depth"
    bucket_size = 60

    # Mock SQL result
    mock_df = Mock()
    spark.sql.return_value = mock_df

    # Mock Delta merge
    mock_target = Mock()
    mock_merge = Mock()
    mock_target.alias.return_value = mock_target
    mock_target.merge.return_value = mock_merge
    mock_merge.whenMatchedUpdateAll.return_value = mock_merge
    mock_merge.whenNotMatchedInsertAll.return_value = mock_merge
    mock_merge.execute.return_value = None

    mock_read_delta_table.return_value = mock_target

    # Run
    result = resample_gold_dts(
        spark,
        logger,
        source_table,
        partition_cols,
        dest_table,
        bucket_size
    )

    assert result is True
    spark.sql.assert_called_once()
    logger.info.assert_called()
    mock_merge.execute.assert_called_once()

@patch("load_resample_gold_dts_test.read_delta_table")
def test_resample_gold_dts_returns_false_on_empty_query(mock_read_delta_table):
    spark = Mock()
    logger = Mock()
    source_table = "my_source"
    dest_table = "my_dest"
    partition_cols = "asset_id,depth"
    bucket_size = 60

    # Patch generate_resample_dts_query_by_bucket to return empty string
    with patch("load_resample_gold_dts_test.generate_resample_dts_query_by_bucket", return_value=""):
        result = resample_gold_dts(
            spark,
            logger,
            source_table,
            partition_cols,
            dest_table,
            bucket_size
        )
        assert result is False
        logger.warning.assert_called()

@patch("load_resample_gold_dts_test.read_delta_table")
def test_resample_gold_dts_handles_exception(mock_read_delta_table):
    spark = Mock()
    logger = Mock()
    source_table = "my_source"
    dest_table = "my_dest"
    partition_cols = "asset_id,depth"
    bucket_size = 60

    # Patch generate_resample_dts_query_by_bucket to raise Exception
    with patch("load_resample_gold_dts_test.generate_resample_dts_query_by_bucket", side_effect=Exception("fail")):
        with pytest.raises(Exception):
            resample_gold_dts(
                spark,
                logger,
                source_table,
                partition_cols,
                dest_table,
                bucket_size
            )
        logger.error.assert_called()