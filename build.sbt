name := "DataStreamingPipeline"

version := "0.1"

scalaVersion := "2.11.8"

val confluentVersion = "5.3.0"
val sparkVersion = "2.4.4"

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"
libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % confluentVersion
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % confluentVersion
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11"% "2.8.7"