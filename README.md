# End-to-end-Data-streaming-pipeline
Implementing an end-to-end real-time Data Streaming pipeline using **Scala, Hive, HDFS, Spark, Kafka and Schema Registry**.

**Input**

**Trips**

The main data for this pipeline is historical data of trips. We will create a stream and process this stream
in our pipeline.To prepare data, go to the BiXi website (link provided in the documentation section) and under “Trip
History” section, you will find links to the data from previous year all the way back to 2014. Download
the data of the previous year. There is one file per month. Take the last month. The files are quite big, so
we just take a sample. Extract the first 100 lines of the data after skipping the first line which is the CSV
header. In Linux or Mac, you can run:

head -n 101 <filename> | tail -n 100 > 100_trips.csv

Prepare a command line to produce the content of 100_trips.csv file to Kafka. You can use Kafka console
producer command or implement a tool of your choice. It is better to be able to produce data in small
batches e.g. 10 lines. It gives a better chance to monitor your pipeline.

**Enriched Station Information**

This is available in the repository as EnrichedStationInformation.csv

**Prerequisites**

In order to start the project, you need to prepare the environment which is

• Create required Kafka topics

• Prepare the input

• Register Avro schema

**Kafka**

Create required Kafka topics:

• The input topic called trip that stores trip information in CSV format.

• The topic to hold enriched trip information in Avro format that is called enriched_trip

For all the topics, set the number of partitions to 1 and the replication factor to 1 as well.

**Prepare input**

Set a mechanism to read trip information and produced to input topic called trip.

**Avro schema**

Create the Avro schema for the enriched trip should be registered under enriched_trip-value subject.

**Curator**

Implement a Spark application to curate trips with the enriched station information.

• Spark SQL: use Spark SQL to read the enriched station information from HDFS in the form of CSV.

• Spark Streaming: stream Kafka topic of trip into Spark DStream using Spark Streaming
component. For each RDD in DStream, join it with the enriched station information RDD
created in the previous step. The result would be a curated trip in CSV format.

• Output: the output is the Kafka topic of enriched_trip.

**Schema**

**Trip**

**Field name Type**

start_date String

start_station_code Integer

end_date String

end_station_code Integer

duration_sec Integer

is_member Integer

**Enriched Station Information**

**Field name Type**

system_id String

timezone String

station_id Integer

name String

short_name String

lat Double

lon Double

capacity Integer

**Enriched Trip**

**Field name Type**

start_date String

start_station_code Integer

end_date String

end_station_code Integer

duration_sec Integer

is_member Integer

system_id String

timezone String

station_id Integer

name String

short_name String

lat Double

lon Double

capacity Integer

**Provided documentation and artifacts**

https://www.bixi.com/en/page-27
