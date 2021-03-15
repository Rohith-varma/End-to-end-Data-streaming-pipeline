package ca.bigdata.rohith.datastreamingpipeline

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object Producer extends App with Log {
  //Kafka Producer Configuration
  val producerConfig = new Properties()
  producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfig)

  val tripCsv = "/home/rohith/Downloads/100_trips.csv"
  val topicName = "winter2020_rohith_trip"
  val source = Source.fromFile(tripCsv)

  println("| Key | Value | Topic | Partition |")
  for (csvline <- source.getLines()) {
    {
      val key = csvline.split(",") {
        0
      } //extracting the Key
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, csvline)
      //send the record to Kafka topic
      producer.send(record)
      println(s"|${record.key()}| ${record.value()}| ${record.topic()}| ${record.partition()}")
    }
  }
  producer.flush()
  producer.close()
}
