from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Example kafka topics, servers and credentials
KAFKA_TOPIC_ONE = "KAFKA_TOPIC_ONE"
KAFKA_TOPIC_TWO = "KAFKA_TOPIC_TWO"
KAFKA_BOOTSTRAP_SERVERS_CONS = "10.10.110.10:9092"

SparkAppName = "spark_streaming_example"
broker = "10.10.110.10"
driverPort = "33527"
localhost = "127.0.0.1"
uiPort = "4040"
aclsUser = "username"
aclsGroup = "groupname"
aclsUserPsw = "password"

# Initiating SparkSession
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName(SparkAppName) \
    .config("spark.driver.host", localhost) \
    .config("spark.driver.port", "9042") \
    .config("spark.driver.bindAddress", localhost) \
    .config("spark.ui.port", uiPort) \
    .config("spark.authenticate", "true") \
    .config("spark.acls.enable", "true") \
    .config("spark.admin.acls", aclsUser) \
    .config("spark.admin.acls.groups", aclsGroup) \
    .config("spark.authenticate.secret", aclsUserPsw) \
    .config("spark.cassandra.connection.host", broker) \
    .config("spark.executor.heartbeatInterval", "20") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Function to dejsonify the kafka stream
def dejson_spark(kafka_topic, spark_schema):
    parsed = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(F.from_json(F.col("value").cast("string"), spark_schema).alias("parsed_value"))

    streamdf = parsed.select("parsed_value.*")

    return streamdf


# Schema for first kafka topic
schema_one = StructType() \
    .add("example_field_one", TimestampType()) \
    .add("example_field_two", StringType()) \
    .add("example_field_three", IntegerType()) \
    .add("example_field_four", IntegerType()) \
    .add("example_field_five", StringType()) \
    .add("example_field_six", StringType()) \
    .add("example_field_seven", StringType()) \
    .add("join_value", StringType())

streamdf_one = dejson_spark(KAFKA_TOPIC_ONE, schema_one)

# Schema for second kafka topic
schema_two = StructType() \
    .add("example_field_x_one", IntegerType()) \
    .add("example_field_x_two", TimestampType()) \
    .add("example_field_x_three", StringType()) \
    .add("join_value", StringType()) \
    .add("example_field_x_four", TimestampType()) \
    .add("example_field_x_five", TimestampType()) \
    .add("example_field_x_six", StringType()) \
    .add("example_field_x_seven", StringType()) \
    .add("example_field_x_eigth", StringType()) \
    .add("example_field_x_nine", StringType()) \
    .add("example_field_x_ten", StringType()) \
    .add("example_field_x_eleven", IntegerType()) \

streamdf_two = dejson_spark(KAFKA_TOPIC_TWO, schema_two)


def process_one(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="example_table_one", keyspace="example_keyspace") \
        .save()


def process_two(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="example_table_two", keyspace="example_keyspace") \
        .save()


query_one = streamdf_one.writeStream \
                        .foreachBatch(process_one) \
                        .start()

query_two = streamdf_two.writeStream \
                        .foreachBatch(process_two) \
                        .start()

def process_joined(df, epoch_id):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode('append') \
      .options(table="example_table_joined", keyspace="example_keyspace") \
      .save()


stream_joined = streamdf_one.join(streamdf_two, ["join_value"])

joined_query = stream_joined.writeStream \
                            .foreachBatch(process_joined) \
                            .start()

spark.streams.awaitAnyTermination()

