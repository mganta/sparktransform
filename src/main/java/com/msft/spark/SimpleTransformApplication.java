package com.msft.spark;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Encoders;

public class SimpleTransformApplication {
    public static void main(String[] args) throws StreamingQueryException {
        //set log4j programmatically
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        if (args.length < 3) {
            System.err.println("Usage: SimpleTransformApplication kafka-url incomingtopic outgoingtopic");
            System.exit(1);
        }

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("SimpleTransformApplication");

        //initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        //dataset from kafka
        Dataset<String> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", args[1])
                .option("subscribe", args[2])
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING());

        //apply transformation
        ds1.foreach((ForeachFunction<String>) fileName -> {
            System.out.println(fileName);
            TransformFunction.transform(fileName);
        });

        //dataset to kafka
        ds1.selectExpr("CAST(current_timestamp() AS STRING) AS key", "CAST(value AS STRING) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", args[1])
                .option("topic", args[3])
                .start();

        //sample downstream processing with a count
        StreamingQuery query1 = ds1
                .groupBy("value")
                .count()
                .writeStream()
                .queryName("Count Query")
                .outputMode("complete")
                .format("console")
                .start();

        query1.awaitTermination();
    }
}
