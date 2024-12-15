package no.hiof.scala_pums
import org.neo4j.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.neo4j.spark.DataSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.format_number

object Neo4j {

  def testSpark(): Unit = {

    val url = "bolt://neo4j:7687"
    val username = "neo4j"
    val password = "testing123"
    val dbname = "neo4j"

    val spark = SparkSession.builder
        .config("neo4j.url", url)
        .config("neo4j.authentication.basic.username", username)
        .config("neo4j.authentication.basic.password", password)
        .config("neo4j.database", dbname)
        .getOrCreate()


    // val dataset = Spark.getDataset("crime_data.csv", spark)
    val homicide_data = Spark.getDataset("homicide-data.csv", spark)

    // dataset.show(5)
    homicide_data.show(5)
    println("Hello!")

    println("Writing to Neo4j database...")
    // write_homicide(homicide_data, spark)
    key_write_strategy(homicide_data, spark)
    val test_query = """
      MATCH (n) RETURN n
    """
    println(s"Running query: \n$test_query")

    query_read(test_query, spark).show()


    println("After Query")
  }

  def query_read(query_string: String, spark: SparkSession): DataFrame = {

    val df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("query", query_string)
        .load()

    df
  }

  
  def write_homicide(data: DataFrame, spark: SparkSession): Unit = {

    // Create Victim Nodes
    // data.select("uid", "victim_last", "victim_first", "victim_race", "victim_age", "victim_sex")
    //   .write
    //   .format("org.neo4j.spark.DataSource")
    //   .mode("overwrite")
    //   .option("labels", "Victim")
    //   .option("node.keys", "uid")
    //   .save()
    //
    // // Create Incident Nodes
    // data.select("uid", "reported_date", "disposition", "city", "state", "lat", "lon")
    //   .write
    //   .format("org.neo4j.spark.DataSource")
    //   .mode("overwrite")
    //   .option("labels", "Incident")
    //   .option("node.keys", "uid")
    //   .save()
    //
    // // Create Location Nodes
    // data.select("city", "state", "lat", "lon")
    //   .distinct()
    //   .write
    //   .format("org.neo4j.spark.DataSource")
    //   .mode("overwrite")
    //   .option("node.keys", "city,state")
    //   .option("labels", "Location")
    //   .save()

    // val reformatDf = data
    //   .withColumn("source.id", $"uid")
    //   .withColumn("source.name", concat(col("victim_last"), lit(", "), col("victim_first")))
    //   .withColumn("source.race", $"victim_race")
    //   .withColumn("source.sex", $"victim_sex")
    //   .withColumn("source.age", $"victim_age")
    //   .withColumn("target.id", $"uid")
    //   .withColumn("rel.city", $"city")
    //   .withColumn("rel.")

    // val victim_nodes = data.select(
    //   $"uid".as("personId"), // Key
    //   $"victim_last".as("last_name"),
    //   $"victim_first".as("first_name"),
    //   $"victim_race".as("race"),
    //   $"victim_age".as("age"),
    //   $"victim_sex".as("gender")
    // )
    //
    // victim_nodes


    println("Initial Schema:")
    data.printSchema()
    // |-- uid: string (nullable = true)
    // |-- reported_date: integer (nullable = true)
    // |-- victim_last: string (nullable = true)
    // |-- victim_first: string (nullable = true)
    // |-- victim_race: string (nullable = true)
    // |-- victim_age: string (nullable = true)
    // |-- victim_sex: string (nullable = true)
    // |-- city: string (nullable = true)
    // |-- state: string (nullable = true)
    // |-- lat: double (nullable = true)
    // |-- lon: double (nullable = true)
    // |-- disposition: string (nullable = true)

    println("Writing Victim -> Incident Relationship")
    val victim_to_incident = data
      .withColumn("sourceID", col("uid"))// Renaming for the source node
      .withColumn("targetID", col("uid"))  // Adding target node column explicitly
      .withColumn("rel.disposition", lit(null)) // Optional: Adding relationship property column (if needed)

    // Validate Schema
    victim_to_incident.printSchema()

    // Write to Neo4j
    victim_to_incident.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Append)
      .option("relationship", "VICTIM_OF")
      .option("relationship.save.strategy", "keys")
      // Match source nodes with the specified label
      .option("relationship.source.save.mode", "Match")
      .option("relationship.source.labels", "Victim")
      .option("relationship.source.node.keys", "sourceID:id")
      .option("relationship.target.labels", "Incident")
      .option("relationship.target.node.keys", "targetID:id")
      .save()

    println("Successfully written Victim -> Incident relationship edges")

    return
    println("Writing Incident -> Location")
    // Incident -[:OCCURRED_AT] -> Location
    val incident_to_location = data
      .withColumnRenamed("uid", "source.id")
      .withColumnRenamed("city", "target.city")
      .withColumnRenamed("state", "target.state")

    incident_to_location.write
      .format("org.neo4j.spark.DataSource")
      .mode("overwrite")
      .option("relationship", "OCCURED_AT")
      .option("relationship.source.labels", ":Incident")
      .option("relationship.source.node.keys", "id")
      .option("relationship.target.labels", ":Location")
      .option("relationship.target.node.keys", "city,state")
      .save() 


  }

    // |-- uid: string (nullable = true)
    // |-- reported_date: integer (nullable = true)
    // |-- victim_last: string (nullable = true)
    // |-- victim_first: string (nullable = true)
    // |-- victim_race: string (nullable = true)
    // |-- victim_age: string (nullable = true)
    // |-- victim_sex: string (nullable = true)
    // |-- city: string (nullable = true)
    // |-- state: string (nullable = true)
    // |-- lat: double (nullable = true)
    // |-- lon: double (nullable = true)
    // |-- disposition: string (nullable = true)

  def key_write_strategy(data: DataFrame, spark: SparkSession): Unit = {

    println("Writing with Key Strategy")

    val locationDf = data.select("state", "city").distinct()

    locationDf.write
          .format("org.neo4j.spark.DataSource")
          .mode(SaveMode.Overwrite)
          .option("labels", ":Location")
          .option("node.keys", "city, state")
          // .save()

    val victimDf = data.select("uid", "victim_last", "victim_first", "victim_race", "victim_age", "victim_sex")

    victimDf.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Append)
      .option("labels", ":Victim")
      // .save()


    val incidentDf = data.select("uid", "reported_date", "city", "state", "disposition")

    incidentDf.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Append)
      .option("labels", ":Incident")
      // .save()

    data.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("relationship", ":VICTIM_OF")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.source.labels", ":Victim")
      .option("relationship.source.node.keys", "uid")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.target.labels", ":Incident")
      .option("relationship.target.node.keys", "uid")
      // .save()

    data.select("uid", "city", "state")
      .write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("relationship", ":OCCURED_AT")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.source.labels", ":Incident")
      .option("relationship.source.node.keys", "state")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.target.labels", ":Location")
      .option("relationship.target.node.keys", "state")
      .option("batch.size", "100")
      .save()

      println("Incident to location completed")
  }



}

