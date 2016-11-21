# KafkaSparkStreaming
To Process Requests from Kafka to Query HBase using Spark Streaming

Spark Streaming Code to stream the kafka messages and query hbase for the rowkey found in the kafka message
send result back to kafka topic.

Basically the request can be in any format parsing logic can be added as per need, intitally i developed this for XML messages
but for simpliciy i removed that logic and made it as simple text messages.

This whole POC challenge was to we have to develop on specific spark and cloudera versions which did not had support
for HBaseContext.


Version Details :

    <spark.version>1.5.0-cdh5.5.1</spark.version>
    <kafka.version>0.9.1</kafka.version>
    <scala.version>2.10.0</scala.version>
    <hadoop.version>2.6.0-cdh5.5.1</hadoop.version>
    
