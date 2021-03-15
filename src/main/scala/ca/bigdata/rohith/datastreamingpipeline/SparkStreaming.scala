package ca.bigdata.rohith.datastreamingpipeline

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App with Log {

  //Initialize Spark Session
  val spark = SparkSession.builder().appName("Trip Enricher")
    .master("local[*]").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val filepath = "hdfs://quickstart.cloudera:8020/user/winter2020/rohith/final_project/enriched_station_information/"

  //esInfo Schema
  val esInfoSchema = StructType(
    List(
      StructField("system_id", StringType, nullable = true),
      StructField("timezone", StringType, nullable = true),
      StructField("station_id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("short_name", StringType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true),
      StructField("capacity", IntegerType, nullable = true)
    )
  )
  val enrichedStationInfoDf: DataFrame = spark.read.format("csv").schema(esInfoSchema).csv(filepath)
    .withColumn("id1", monotonically_increasing_id())

  //kafka configuration for streaming
  val kafkaConf = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "application-group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  //creating the stream and subscribing to the topic
  val topic = "winter2020_rohith_trip"
  val kafkaStream: InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConf)
    )=
  }

  //extracting Values from Kafka Stream
  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  kafkaStreamValues.foreachRDD(tripRdd => enrich(tripRdd))

  def enrich(tripRdd: RDD[String]): Unit = {
    import spark.implicits._
    val tripDf = tripRdd.map(Trip(_))
      .toDF("start_date", "start_station_code", "end_date", "end_station_code", "duration_sec", "is_member")
      .withColumn("id2", monotonically_increasing_id())

    val enrichedTripRdd = tripDf.join(enrichedStationInfoDf, col("id1") === col("id2"), "inner")
      .drop("id1", "id2").rdd

    //Schema registry configuration
    //Create the Schema Registry Client
    val srClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 3)
    //Get the details of the Subject
    val metadata = srClient.getSchemaMetadata("winter2020_rohith_enriched_trip-value", 1)
    //Retrieve the Id of the Schema registry subject
    val avroSchemaId = metadata.getId
    //Get the schema by Id
    val avroSchema = srClient.getByID(avroSchemaId)
    //Converting schema to Struct Type
    val schemaStructType = SchemaConverters.toSqlType(avroSchema)
      .dataType.asInstanceOf[StructType]

    val enrichedTripDf = spark.createDataFrame(enrichedTripRdd, schemaStructType).as("enTripDf")

    val streamingData = enrichedTripDf.select("enTripDf.*").map(row => row.mkString(",")).toDF("value")

    streamingData
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "winter2020_rohith_enriched_trip")
      .save()

    println("Below is the sample of Streamed Data")
    streamingData.show(5)
    println("Streaming Successful:Above batch of Enriched Trip Data has been written to winter2020_rohith_enriched_trip Kafka topic")
  }

  println("Started Spark Streaming to Kafka Topic winter2020_rohith_enriched_trip")
  ssc.start()
  ssc.awaitTerminationOrTimeout(100)
  ssc.stop(stopSparkContext = true, stopGracefully = true)
}
