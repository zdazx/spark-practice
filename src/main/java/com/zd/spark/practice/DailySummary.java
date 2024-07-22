package com.zd.spark.practice;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

public class DailySummary {
    public static void main(String[] args) {
        // create spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Daily Summary")
                .master("local[*]")
                .getOrCreate();

        // create DataFrames from an existing RDD
        // extract
        String path = "/Users/duo.zhang/Downloads/backblaze/*/*.csv";
        Dataset<Row> extractedDF = sparkSession.read().option("header", "true").csv(path);

        // transform
        Dataset<Row> cleanedDF = extractedDF.where("date is not null").select("date", "failure");
        Dataset<Row> transformedDF = cleanedDF.groupBy("date").agg(
                count("*").alias("drive_count"),
                sum("failure").alias("drive_failures_count")
        );
//        transformedDF.show();

        // load
        transformedDF.repartition(1).write().option("header", "true").csv("output/daily_summary");

        // close spark session
        sparkSession.stop();
    }
}
