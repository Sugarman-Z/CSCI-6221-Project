package com.gwu

import breeze.numerics.sqrt
import com.gwu.offlineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("offlineRecommender")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()

    // divide data to testing set and training set
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // core implementation: output optimize parameter
    adjustALSParams( trainingRDD, testingRDD )

    spark.stop()
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // traverse the parameters defined in array
    val result = for ( rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01) )
      yield {
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE( model, testData )
        ( rank, lambda, rmse )
      }
    // order by rmse and print the best parameters
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // construct userProducts in order to get prediction rating matrix
    val userProducts = data.map( item => (item.user, item.product) )
    val predictRating = model.predict(userProducts)

    // use formula to calculate rmse
    // firstly, join predict rating table and actual rating table by (userId, productId)
    val observed = data.map( item => ( (item.user, item.product), item.rating ) )
    val predicted = predictRating.map( item => ( (item.user, item.product), item.rating ) )

    sqrt(
      observed.join(predicted).map{
        case ( (userId, productId), (actual, predict) ) =>
          val error = actual - predict
          error * error
      }.mean()
    )
  }
}
