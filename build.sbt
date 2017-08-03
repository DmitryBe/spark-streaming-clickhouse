name := """spark-streaming-clickhouse"""

version := "1.0"

scalaVersion := "2.11.7"

val sparkV = "2.2.0"
val spark = Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkV,
  "org.apache.spark" % "spark-sql_2.11" % sparkV,
  "org.apache.spark" % "spark-streaming_2.11" % sparkV
)

val clickhouse = Seq(
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.1.25"
)

val scalaExt = Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.7",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

libraryDependencies ++= scalaExt ++ spark ++ clickhouse

// spark likes this version
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"

packAutoSettings