package org.example.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Example {

  def main(args: Array[String]) {

    if (args.length > 1) {
      System.err.println("Usage: spark2-submit --class org.example.processing.Example --master yarn target/example-processing-1.0-jar-with-dependencies.jar")
      System.exit(1)
    }

    // Initialize spark context
    val spark = SparkSession.builder.appName("Processing - Example").getOrCreate()

    // Import implicit functions
    import spark.implicits._

    // Load all factory data
    val data = spark.read.parquet("/datalake/common/raw/manufacturing/factory")

    // Group data by factory,machine,type and count number of events by groups
    val grouped = data.groupBy($"FACTORY", $"MACHINE", $"TYPE").agg(count("*").alias("TOTAL_BY_TYPE"))

    // write results to parquet
    grouped.write.parquet("/datalake/common/example/storage_status")
  }
}
