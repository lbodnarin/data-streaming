from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, date_format, substring, window
from pyspark.sql.types import DoubleType, StringType, StructType, TimestampType
import argparse
import boto3
import os

TABLE_NAME = "DASHBOARD"
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

class ForeachWriter:
    def open(self, partition_id, epoch_id):
        self.dyn_resource = boto3.session.Session(region_name=AWS_DEFAULT_REGION).resource("dynamodb")
        return True
    def process(self, row):
        self\
            .dyn_resource\
            .Table(TABLE_NAME)\
            .update_item(
                Key={"DATE": row["DATE"], "IATA": row["IATA"]},
                UpdateExpression="SET #C = :c",
                ExpressionAttributeNames={"#C": "COUNT"},
                ExpressionAttributeValues={":c": row["COUNT"]}
            )
    def close(self, error):
        if error:
            raise error

def streamingDF(args):
    conf = SparkConf()\
        .set("spark.sql.shuffle.partitions", 2)\
        .set("spark.sql.streaming.noDataMicroBatches.enabled", False)
    with SparkSession.builder.config(conf=conf).getOrCreate() as ss:
        struct = StructType()\
            .add("LOG_FORMAT_VERSION", DoubleType())\
            .add("QUERY_TIMESTAMP", TimestampType())\
            .add("HOSTED_ZONE_ID", StringType())\
            .add("QUERY_NAME", StringType())\
            .add("QUERY_TYPE", StringType())\
            .add("RESPONSE_CODE", StringType())\
            .add("LAYER_4_PROTOCOL", StringType())\
            .add("ROUTE_53_EDGE_LOCATION", StringType())\
            .add("RESOLVER_IP_ADDRESS", StringType())\
            .add("EDNS_CLIENT_SUBNET", StringType())
        sq = ss\
            .readStream\
            .format("csv")\
            .option("mode", "FAILFAST")\
            .option("header", False)\
            .option("delimiter", " ")\
            .option("recursiveFileLookup", True)\
            .option("path", args.INPUT)\
            .schema(struct)\
            .load()\
            .withWatermark("QUERY_TIMESTAMP", "1 day")\
            .dropDuplicates(["QUERY_TIMESTAMP", "ROUTE_53_EDGE_LOCATION"])\
            .groupBy(window("QUERY_TIMESTAMP", "1 day"), "ROUTE_53_EDGE_LOCATION")\
            .agg(count("*").alias("COUNT"))\
            .select(date_format("window.start", "yyyy-MM-dd").alias("DATE"), substring("ROUTE_53_EDGE_LOCATION", 1, 3).alias("IATA"), "COUNT")\
            .writeStream\
            .foreach(ForeachWriter())\
            .option("checkpointLocation", args.checkpointLocation)\
            .outputMode("update")\
            .trigger(processingTime="5 minutes")\
            .start()
        sq.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--I", dest="INPUT")
    parser.add_argument("--C", dest="checkpointLocation")
    args = parser.parse_args()
    my_session = boto3.session.Session()
    if TABLE_NAME not in my_session.client("dynamodb").list_tables()["TableNames"]:
        my_session\
            .resource("dynamodb")\
            .create_table(
                TableName=TABLE_NAME,
                KeySchema=[{"AttributeName": "DATE", "KeyType": "HASH"}, {"AttributeName": "IATA", "KeyType": "RANGE"}],
                AttributeDefinitions=[{"AttributeName": "DATE", "AttributeType": "S"}, {"AttributeName": "IATA", "AttributeType": "S"}],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
            )\
            .wait_until_exists()
    streamingDF(args)