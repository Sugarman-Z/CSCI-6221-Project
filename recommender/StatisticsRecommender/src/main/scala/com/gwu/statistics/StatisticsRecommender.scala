package com.gwu.statistics

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  // define mongo table name
  val MONGODB_RATING_COLLECTION = "Rating"

  // statistics table name
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // create temporary ratings view for spark sql
    ratingDF.createOrReplaceTempView("ratings")

    // use spark sql to do various statistic recommendation
    // 1. history hot product, use rating counts. (productId, count)
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count " +
      "from ratings " +
      "group by productId " +
      "order by count desc")
    storeDFInMongoDB( rateMoreProductsDF, RATE_MORE_PRODUCTS )

    // 2. recent hot product, use timestamp (format timestamp -> yyyyMM) (productId, count, yearmonth)
    // create a date format tool
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // register UDF, and format timestamp as yyyyMM
    spark.udf.register("changeDate", (x: Int) => {
      simpleDateFormat.format(new Date(x * 1000L)).toInt
    })
    // convert raw data into desired structure
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth " +
      "from ratingOfMonth " +
      "group by yearmonth, productId " +
      "order by yearmonth desc, count desc")
    // store df to mongoDB
    storeDFInMongoDB( rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS )

    // 3. high quality product, use product rating
    val averageProductsDF = spark.sql("select productId, avg(score) as avg " +
      "from ratings " +
      "group by productId " +
      "order by avg desc")
    storeDFInMongoDB( averageProductsDF, AVERAGE_PRODUCTS )

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
