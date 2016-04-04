package sparktraining

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val inputPath = "src/main/resources/data/shakespeare.txt"
    val outputPath = "target/output/wordcount"

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    try {
      val file = sc.textFile(inputPath)
      val wordCount = file
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .map(word => (word, 1))
        .reduceByKey((accumulator, count) => accumulator + count)
      //.reduceByKey(_ + _)
      println(s"Writing output to: $outputPath")
      wordCount.saveAsTextFile(outputPath)
    } finally {
      sc.stop()
    }
  }
}
