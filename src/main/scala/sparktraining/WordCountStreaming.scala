package sparktraining

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCountStreaming {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf()
      //.setMaster("spark://localhost:7077")
      .setMaster("local[4]")
      .set("spark.eventLog.enabled", "true")
      .setAppName("WordCountStreaming")

    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    val wordCounts = streamingContext.socketTextStream("localhost", 9999)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts.saveAsTextFiles("words", "count")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
