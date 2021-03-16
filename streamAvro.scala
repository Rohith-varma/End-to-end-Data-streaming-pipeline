package ca.bigdata.rohith.datastreamingpipeline

import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import io.confluent.kafka.serializers.KafkaAvroSerializer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import collection.JavaConverters._

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
    )
  }
  //extracting Values from Kafka Stream
  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  kafkaStreamValues.foreachRDD(tripRdd => enrich(tripRdd))

  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty("schema.registry.url", "http://172.16.129.58.:8081")

  val producer = new KafkaProducer[String,GenericRecord](producerProperties)

  //Schema registry configuration
  //Create the Schema Registry Client
  val srClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 3)
  //Get the details of the Subject
  val metadata = srClient.getSchemaMetadata("winter2020_rohith_enriched_trip-value", 1)
  //Retrieve the Id of the Schema registry subject
  val avroSchemaId = metadata.getId
  //Get the schema by Id
  val avroSchema = srClient.getByID(avroSchemaId)

  def enrich(tripRdd: RDD[String]): Unit = {
    import spark.implicits._
    val tripDf = tripRdd.map(Trip(_))
      .toDF("start_date", "start_station_code", "end_date", "end_station_code", "duration_sec", "is_member")
      .withColumn("id2", monotonically_increasing_id())

    val enrichedTripDf = tripDf.join(enrichedStationInfoDf, col("id1") === col("id2"), "inner")
      .drop("id1", "id2").as("enTripDf")

    val listEnrichedTrip = enrichedTripDf.select("enTripDf.*").map(row => row.mkString(",")).collect().toList

    val avroEnrichedTrip: List[GenericRecord] = listEnrichedTrip.map{ csv =>
      val fields = csv.split(",",-1)
      new GenericRecordBuilder(avroSchema)
        .set("start_date", fields(0))
        .set("start_station_code", fields(1).toInt)
        .set("end_date", fields(2))
        .set("end_station_code", fields(3).toInt)
        .set("duration_sec", fields(4).toInt)
        .set("is_member", fields(5).toInt)
        .set("system_id", fields(6))
        .set("timezone", fields(7))
        .set("station_id", fields(8).toInt)
        .set("name", fields(9))
        .set("short_name", fields(10).toInt)
        .set("lat", fields(11).toDouble)
        .set("lon", fields(12).toDouble)
        .set("capacity", fields(13).toInt)
        .build()
    }
    avroEnrichedTrip
      .map(avroMessage => new ProducerRecord[String,GenericRecord]("winter2020_rohith_enriched_trip", avroMessage))
      .foreach(producer.send)
  }
}
