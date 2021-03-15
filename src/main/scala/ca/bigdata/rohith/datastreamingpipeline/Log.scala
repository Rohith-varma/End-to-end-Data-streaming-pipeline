package ca.bigdata.rohith.datastreamingpipeline

import org.apache.log4j.{Level, Logger}

trait Log {

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark-project").setLevel(Level.WARN)
  Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
}
