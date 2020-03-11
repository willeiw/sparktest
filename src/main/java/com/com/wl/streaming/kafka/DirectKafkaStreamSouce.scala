package com.com.wl.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

object DirectKafkaStreamSouce {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)

    val Array(brokers, topics) = args

    val ssc = new StreamingContext(sc, Second(2))

    //create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //get the lines,split into words,count the word and print
    val line = message.map(_._2)
    val words = line.flatMap(_.split(" "))
    val wordcounts = words.map(x=>(x,1L)).reduceByKey(_ + _)
    wordcounts.print()

    //start computation
    ssc.start()
    ssc.awaitTermination()
  }

}
