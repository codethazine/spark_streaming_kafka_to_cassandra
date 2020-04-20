# Spark Streaming from Kafka to Cassandra
An example for streaming data with Spark Streaming, from Kafka Topics to Cassandra.
I couldnt find any clear examples on the web, so I decided to post how managed to pull it of.

To run the app the script the following command was used:
```
spark-submit --jars jsr166e_1_1_0.jar,spark_sql_kafka_0_10_2_11_2_4_1.jar,spark_cassandra_connector_2_3_1_s_2_11.jar spark_streaming_all.py
```

Remember to put the jar packages in your path.
