package com.zd.spark.practice;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class YearlySummary {
    public static void main(String[] args) {
        // create spark session
        SparkSession sparkSession = SparkSession.builder()
                .appName("Yearly Summary")
                .master("local[*]")
                .getOrCreate();

        // create DataFrames from an existing RDD
        // extract
        String path = "/Users/duo.zhang/Downloads/backblaze/*/*.csv";
        Dataset<Row> extractedDF = sparkSession.read().option("header", "true").csv(path);

        // transform
        Dataset<Row> cleanedDF = extractedDF.where("date is not null")
                .select(
                        year(col("date")).alias("year"),
                        new Column("model"),
                        new Column("failure"));

        Dataset<Row> cleanedDFByBrand = cleanedDF.withColumn("model",
                when(col("model").startsWith("CT"), "Crucial").
                        when(col("model").startsWith("DELLBOSS"), "Dell BOSS").
                        when(col("model").startsWith("HGST"), "HGST").
                        when(col("model").startsWith("Seagate"), "Seagate").
                        when(col("model").startsWith("ST"), "Seagate").
                        when(col("model").startsWith("Toshiba"), "TOSHIBA").
                        when(col("model").startsWith("WDC"), "Western Digital").
                        otherwise("Others")
        );

        Dataset<Row> transformedDF = cleanedDFByBrand.groupBy("year", "model").agg(
                sum("failure").alias("drive_failures_count")
        ).sort("year","model");

//        transformedDF.show();

        // load
        transformedDF.repartition(1).write().option("header", "true").csv("output/yearly_summary");

        // close spark session
        sparkSession.stop();
    }
}
