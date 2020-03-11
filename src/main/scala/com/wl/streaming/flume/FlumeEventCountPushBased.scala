package com.wl.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object FlumeEventCountPushBased {
  def main(args: Array[String]) = {
    if (args.length < 2){
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(2000)

    //create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    //create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

    //print out the count of events received from this server in each batch
    stream.count().map(cnt => "Recevied "+ cnt +" flume events.").print()

    ssc.start()
    ssc.awaitTermination()
  }

}
