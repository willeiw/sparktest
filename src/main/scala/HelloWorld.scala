import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val logFile = "D:\\spark-2.4.3-bin-hadoop2.7\\spark-2.4.3-bin-hadoop2.7\\helloSpark.txt"
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(logFile)
    val counts = rdd.flatMap(line=>line.split(",")).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
    counts.foreach(println)
    sc.stop()
    }

}
