package sparktraining

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext


object MovieLensDataFrame {

  case class Movie(movieId: String, title: String, genres: Seq[String])

  case class User(userId: String, name: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("MovieLens")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val usersDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movielens/users.csv")

    val moviesDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movielens/movies.csv")

    val ratingsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movielens/ratings.csv")

    // Register tables
    ratingsDF.registerTempTable("ratings")
    moviesDF.registerTempTable("movies")
    usersDF.registerTempTable("users")

    // Show the top 10 most-active users and how many times they rated a movie
    val mostActiveUsers = sqlContext
      .sql(
        """SELECT users.name, count(*) as ct from users
          |join ratings on ratings.userId = users.id
          |group by users.name order by ct desc limit 10
        """.stripMargin)

    // Or
    // usersDF.join(ratingsDF).....


    mostActiveUsers.write
      .format("com.databricks.spark.csv")
      .save("target/output/movielens/most_active_users")

    sc.stop()

  }
}
