import pytest
import pyspark.sql.functions as sfunc
import sys
import os
sys.path.insert(0, os.getcwd())
from etl.utils import (
    get_spark, get_task_parameters)
from etl.transactions_etl import get_claims, get_contracts, get_transactions


@pytest.fixture(scope="module")
def get_claims_for_tests():
    spark = get_spark()
    parameters = get_task_parameters()[0].get('sources')
    df_claims = get_claims(spark, parameters)
    return df_claims


@pytest.fixture(scope="module")
def get_contracts_for_tests():
    spark = get_spark()
    parameters = get_task_parameters()[0].get('sources')
    df_contracts = get_contracts(spark, parameters)
    return df_contracts


@pytest.fixture(scope="module")
def get_transactions_for_tests():
    spark = get_spark()
    parameters = get_task_parameters()
    df_transactions = get_transactions(spark, parameters)
    return df_transactions


@pytest.mark.parametrize(
        "desired_schema", [
            [
                "source_system",
                "claim_id",
                "contract_source_system",
                "contract_id",
                "type_of_claim",
                "loss_date",
                "amount",
                "creation_date"
            ]
        ]
)
def test_claims_schema(desired_schema, get_claims_for_tests):
    actual_cols = get_claims_for_tests.columns
    assert all([a == b for a, b in zip(actual_cols, desired_schema)])


@pytest.mark.parametrize(
        "desired_schema", [
            [
                "source_system",
                "contract_id",
                "contract_type",
                "insured_from_date",
                "insured_to_date",
                "creation_date"
            ]
        ]
)
def test_contracts_schema(desired_schema, get_contracts_for_tests):
    actual_cols = get_contracts_for_tests.columns
    assert all([a == b for a, b in zip(actual_cols, desired_schema)])


@pytest.mark.parametrize(
        "desired_schema", [
            [
                "contract_source_location",
                "contract_source_location_id",
                "source_system_id",
                "transaction_source_type",
                "transaction_type",
                "final_amount",
                "currency",
                "business_date",
                "creation_date",
                "system_ts",
                "hashed_claim_id"
            ]
        ]
)
def test_transactions_schema(desired_schema, get_transactions_for_tests):
    actual_cols = get_transactions_for_tests.columns
    assert all(
        [a == b for a, b in zip(
            sorted(actual_cols), 
            sorted(desired_schema)
        )
        ]
    )
    assert all([a == b for a, b in zip(
        sorted(actual_cols),
        sorted(desired_schema))])


@pytest.mark.parametrize(
        "not_nullable_cols", [
            [
                "transaction_type",
                "hashed_claim_id"
            ]
        ]
)
def test_transactions_columns_not_null(
    not_nullable_cols,
    get_transactions_for_tests
):
    df_transactions = get_transactions_for_tests
    condition = ' IS NULL OR '.join(str(col) for col in not_nullable_cols) + ' IS NULL'
    count_nulls = df_transactions.filter(condition).count()
    assert count_nulls <= 0, "There are Nulls in Transactions Not nullable columns"


@pytest.mark.parametrize(
        "primary_keys", [
            [
                "contract_source_location",
                "contract_source_location_id",
                "hashed_claim_id"
            ]
        ]
)
def test_transactions_primary_keys_duplicates(
    primary_keys,
    get_transactions_for_tests
):
    df_transactions = get_transactions_for_tests
    total_dupl_count = 0
    for col in primary_keys:
        pk_dupl_count = (
            df_transactions
            .groupBy(
                sfunc.col(col))
            .agg(sfunc.count('*').alias('count'))
            .where(sfunc.col('count') > 1)
            .count()
        )
        total_dupl_count += pk_dupl_count
    assert (total_dupl_count == 0)
