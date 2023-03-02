# Build a Real-time Application on Amazon EMR

This tutorial shows how to process log data in real-time using Amazon EMR.

I've built an end-to-end system on AWS that ingests streaming data, aggregates that data, and persists the aggregated data in DynamoDB.

Here is my [GitHub repo](https://github.com/lbodnarin/data-streaming.git).

## Architecture

The following diagram illustrates the solution architecture:

![AWS_Architecture.png](https://cdn.hashnode.com/res/hashnode/image/upload/v1677853669865/b0751887-e9c6-4323-9c62-d997f642408c.png?width=768)

The solution workflow consists of the following steps:

1. Route 53 is configured to send public DNS query logs to CloudWatch Logs.
1. CloudWatch Logs forwards all the incoming log events that match the filter pattern `"example.com A NOERROR"` to a Kinesis Data Firehose delivery stream.
1. Kinesis Data Firehose invokes a Lambda Blueprint to uncompress the log data.
1. Lambda sends the uncompressed data to Kinesis Data Firehose (`Timeout`: `3 minutes`).
1. Kinesis Data Firehose then sends it to Amazon S3 (`Prefix`: `data-streaming/Firehose/INPUT/`; `ErrorOutputPrefix`: `data-streaming/Firehose/_error/`).
1. Amazon EMR aggregates incoming log data using Spark Structured Streaming.
1. DynamoDB edits the existing item's counts, or adds a new item to the table if it does not already exist.

## Prerequisites

Before you begin, ensure that you have the following prerequisites:

1. Configure Route 53 to start [logging public DNS queries](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/query-logs.html) for a specified hosted zone.
1. Create a Lambda function for data transformation using a [Kinesis Data Firehose Lambda Blueprint](https://docs.aws.amazon.com/firehose/latest/dev/data-transformation.html#lambda-blueprints).
1. Create a [CloudWatch Logs subscription filter for Kinesis Data Firehose](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html#FirehoseExample).
1. You can find the JSON policy documents that you attach to each IAM role in the repo.

## Step 1: Download repo

```shell
git clone https://github.com/lbodnarin/data-streaming.git
```

`OUTPUT`

```shell
data-streaming/
├── EMR/
│   ├── APP.py
│   └── bootstrap-actions.sh
├── IAM/
│   ├── Firehose_PermissionsPolicy.json
│   ├── Firehose_TrustPolicy.json
│   ├── Lambda_PermissionsPolicy.json
│   ├── Lambda_TrustPolicy.json
│   ├── Logs_PermissionsPolicy.json
│   └── Logs_TrustPolicy.json
└── README.md
```

## Step 2: Copy the local directory to S3

```shell
aws s3 cp ./data-streaming/EMR/ s3://my-bucket/data-streaming/EMR/ --recursive
```

## Step 3: Create IAM default roles for EMR

```shell
aws emr create-default-roles
```

## Step 4: Launch an Amazon EMR cluster

```shell
aws emr create-cluster \
--region=region \
--name='My cluster' \
--release-label=emr-6.9.0 \
--applications=Name=Spark \
--instance-type=m4.large \
--instance-count=1 \
--ec2-attributes=KeyName=myEMRKeyPairName,SubnetId=subnet-id \
--use-default-roles \
--bootstrap-actions=Path=s3://my-bucket/data-streaming/EMR/bootstrap-actions.sh \
--log-uri=s3://my-bucket/data-streaming/EMR/_log/ \
--steps=\
Type=SPARK,\
Name=APP_PY,\
ActionOnFailure=CONTINUE,\
Args=\
s3://my-bucket/data-streaming/EMR/APP.py,\
--I,s3://my-bucket/data-streaming/Firehose/INPUT/,\
--C,s3://my-bucket/data-streaming/EMR/_checkpoint/
```

Copy your `ClusterId`.

## Step 5: Check your cluster status

```shell
aws emr describe-cluster \
--cluster-id=myClusterId
```

The State value changes from `STARTING` to `RUNNING` as Amazon EMR provisions the cluster.

## Step 6: Streaming Query

- It computes the `number of DNS queries per day` for each Route 53 edge location.
- After every trigger, the updated counts are written to your `DynamoDB table`.
- By enabling `checkpointing`, you can restart the query after a failure.

```python
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
```

## Step 7: class ForeachWriter

- It initializes a connection where multiple rows can be written out.
- Edits the existing item's counts, or adds a new item to the table.
- The three-letter code corresponds with the `IATA airport code`.

```python
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
```

## Step 8: View the Aggregated Streaming Data

- Open the `Amazon DynamoDB console`.
- In the navigation pane on the left side of the console, choose `Explore items`.
- Choose the `DASHBOARD` table from the table list.

## Step 9: Terminate your cluster

```shell
aws emr terminate-clusters \
--cluster-id=myClusterId
```

When a cluster is shut down, any step not yet completed is canceled and the Amazon EC2 instances in the cluster are terminated.

## Conclusion

After you complete the steps, you can find more in-depth information to create your own real-time application in the [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).