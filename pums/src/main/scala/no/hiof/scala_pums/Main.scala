package no.hiof.scala_pums

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.format_number

object Main {


  def main(args: Array[String]): Unit = {

    val spark = Spark.getSparkConfig()
    import spark.implicits._

    val myDf1 = Spark.getDataset("pums_person.csv", spark)
    val myDf2 = Spark.getDataset("pums_household.csv", spark)

    Spark.test_connection(myDf1, myDf2, spark)

    // myDf1.show()
    // myDf2.show()

  }



}
