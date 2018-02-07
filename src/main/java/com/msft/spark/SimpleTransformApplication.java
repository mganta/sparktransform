package com.msft.spark;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Encoders;

import java.io.IOException;

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
                .option("kafka.bootstrap.servers", args[0])
                .option("subscribe", args[1])
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING());

        //apply transformation
        //need exception handling here
        ds1.writeStream().foreach(new ForeachWriter<String>() {
            @Override
            public boolean open(long partitionId, long version) {
                return true;
            }

            @Override
            public void process(String fileName)  {
                System.out.println(fileName);
                try {
                    TransformFunction.transform(fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void close(Throwable errorOrNull) {
            }
        }).start();

        //dataset to kafka
        ds1.selectExpr("CAST(current_timestamp() AS STRING) AS key", "CAST(value AS STRING) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", args[0])
                .option("topic", args[2])
                .option("checkpointLocation", "/tmp")
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
