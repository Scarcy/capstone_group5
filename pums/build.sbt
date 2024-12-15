// Prosjekt navn
name := "capstone"
version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1",
  "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.2_for_spark_3"
)

Compile / mainClass := Some("no.hiof.scala_pums.Main")
