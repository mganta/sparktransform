# sparktransform
simple transform

Usage:

spark-submit --class com.msft.spark.SimpleTransformApplication --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 spark-transform-1.0-SNAPSHOT.jar broker1:9092,broker2:9092,broker3:9092 inputtopic outputtopic
