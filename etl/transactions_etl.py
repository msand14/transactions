import pyspark.sql.types as ty
import os
from typing import List, Dict
import pyspark.sql.functions as sfunc
import sys
import os
from pyspark.sql import DataFrame

sys.path.insert(0, os.getcwd())
from etl.utils import get_nse_id, write_parquet, read_csv


def get_claims(spark, task_parameters: List[Dict]) -> DataFrame:
    task_parameters_sources = task_parameters[0].get('sources')
    task_parameters_claims = [
        source for source in task_parameters_sources
        if source.get("name") == "claims"][0]
    claims_input_file_path = task_parameters_claims.get("path")
    df = read_csv(
        spark,
        os.path.join(claims_input_file_path, "claims.csv")
    )
    print('\nPrinting Claims...')
    df.show()
    return df


def get_contracts(spark, task_parameters: List[Dict]) -> DataFrame:
    task_parameters_sources = task_parameters[0].get('sources')
    task_parameters_contracts = [
      source for source in task_parameters_sources
      if source.get("name") == "contracts"][0]
    contracts_input_file_path = task_parameters_contracts.get("path")
    df = read_csv(
        spark,
        os.path.join(contracts_input_file_path, "contracts.csv")
    )
    print('\nPrinting Contracts...')
    df.show()
    return df


def get_transactions(
    spark,
    task_parameters: List[Dict],
    b_write: bool = False
):
    df_claims = get_claims(spark, task_parameters)
    df_contracts = get_contracts(spark, task_parameters)

    task_parameters_targets = task_parameters[0].get('targets')
    task_parameters_transactions = [
      target for target in task_parameters_targets
      if target.get("name") == "transactions"][0]
    target_path_transactions = task_parameters_transactions.get("path")

    api_url_claim_id = task_parameters_transactions.get("api_url_claim_id")
    df_contracts = df_contracts.withColumnRenamed(
        "creation_date",
        "con_creation_date"
    )
    df_contracts = df_contracts.withColumnRenamed(
        "contract_id", "con_contract_id"
    )
    df_transactions = (
      df_contracts.alias('con')
      .join(
        df_claims.alias('cla'),
        how='left',
        on=(
            df_claims.contract_id == df_contracts.con_contract_id
        )
      )
    ).drop("con_creation_date")

    get_nse_id_udf = sfunc.udf(
        lambda x: get_nse_id(x, api_url_claim_id), ty.StringType()
    )

    df_transactions = (
      df_transactions
      .withColumnRenamed("con_contract_id", "contract_source_system_id")
      .withColumn("contract_source_system", sfunc.lit("Europe South"))
      .withColumn("claim_id_prefix_and_source_system_id",
                  sfunc.split(sfunc.col("claim_id"), "_"))
      .withColumn("claim_id_prefix",
                  sfunc.col("claim_id_prefix_and_source_system_id")[0])
      .withColumn("source_system_id",
                  sfunc.col("claim_id_prefix_and_source_system_id")[1])
      .withColumn(
        "transaction_source_type",
        sfunc.when(sfunc.col("type_of_claim") == "2", "Corporate")
        .when(sfunc.col("type_of_claim") == "1", "Private")
        .when(sfunc.col("type_of_claim") == "", "Unknown")
      )
      .withColumn(
        "transaction_type",
        sfunc.when(sfunc.col("claim_id_prefix").contains("CL"), "coinsurance")
        .when(sfunc.col("claim_id_prefix").contains("RX"), "reinsurance")
      )
      .withColumnRenamed("amount", "final_amount")
      .withColumnRenamed("currency", "currency")
      .withColumn("business_date",
                  sfunc.to_date(sfunc.col("loss_date"), "dd.MM.yyyy"))
      .withColumn("creation_date",
                  sfunc.to_timestamp("creation_date", "dd.MM.yyyy HH:mm"))
      .withColumn("system_ts", sfunc.current_timestamp())
      .withColumn("hashed_claim_id", get_nse_id_udf(sfunc.col("claim_id")))
    ).drop(*[
        "claim_id_prefix_and_source_system_id",
        "claim_id_prefix",
        "con.creation_date"
      ]
    )

    df_transactions = df_transactions.select(
      *[
          "contract_source_system_id",
          "contract_source_system",
          "source_system_id",
          "transaction_source_type",
          "transaction_type",
          "final_amount",
          "currency",
          "business_date",
          "creation_date",
          "system_ts",
          "hashed_claim_id"]
    )

    df_transactions = (
        df_transactions
        .withColumn(
            "contract_source_system_id",
            sfunc.col("contract_source_system_id").cast(ty.LongType()))
        .withColumn(
            "contract_source_system",
            sfunc.col("contract_source_system").cast("string"))
        .withColumn(
            "source_system_id",
            sfunc.col("source_system_id").cast("int"))
        .withColumn(
            "transaction_source_type",
            sfunc.col("transaction_source_type").cast("string"))
        .withColumn(
            "transaction_type",
            sfunc.col("transaction_type").cast("string"))
        .withColumn(
            "final_amount",
            sfunc.col("final_amount").cast("decimal(16, 5)"))

    )
    if b_write:
        write_parquet(
          df_transactions,
          os.path.join(target_path_transactions, "TRANSACTIONS")
        )

    return df_transactions
