package no.hiof.scala_pums

import org.apache.spark._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.format_number

object Spark {

  def getSparkConfig(): SparkSession = {
    val sparkConf = new SparkConf()
        .setAppName("Capstone")
        .set("spark.cassandra.connection.config.cloud.path", "db_cass.zip")

    val spark = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    spark.conf.set("spark.sql.catalog.cass_catalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def getDataset(name: String, spark: SparkSession): DataFrame = {
    spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .load(s"/capstone/data/$name")

  }

  def test_connection(myDf: DataFrame, myDf2: DataFrame, spark: SparkSession): Boolean = {
    spark.sql("SHOW NAMESPACES FROM cass_catalog").show()
    spark.sql("SHOW TABLES FROM cass_catalog.keyspace_pums").show()
    println("Person Data")
    myDf.printSchema()
    println("Household Data")
    myDf2.printSchema()
    val dataCount = myDf.count()
    println(f"Dataset has $dataCount values")

    dataCount > 0
  }
}
