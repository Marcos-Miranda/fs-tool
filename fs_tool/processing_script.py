import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "sagemaker-feature-store-pyspark-3.2"])

import argparse
from ast import literal_eval
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_dataset_uri", type=str)
    parser.add_argument("--entity_columns", type=str)
    parser.add_argument("--s3_query_bucket", type=str)
    parser.add_argument("--s3_query_key", type=str)
    parser.add_argument("--feat_group_arn", type=str)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("FeatureCalculation").getOrCreate()
    fsm = FeatureStoreManager()

    # read the dataset and create a new column to it
    # if there are more than 1 entity column (they need to be concatenated)
    df = spark.read.csv(args.s3_dataset_uri, header=True)
    entity_columns = literal_eval(args.entity_columns)
    if len(entity_columns) == 2:
        df = df.withColumn("_".join(entity_columns), concat(df[entity_columns[0]], lit("_"), df[entity_columns[1]]))

    # retrieve that query that calculates features and run it
    df.createOrReplaceTempView("table")
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name=args.s3_query_bucket, key=args.s3_query_key)
    query = obj.get()["Body"].read().decode("utf-8")
    df_output = spark.sql(query).limit(10)

    # ingest the calculated features in the Feature Store
    fsm.ingest_data(df_output, args.feat_group_arn, target_stores=["OfflineStore", "OnlineStore"])


if __name__ == "__main__":
    main()
