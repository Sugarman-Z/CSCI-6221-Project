package com.gwu

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


case class ProductRating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Long
                 )

case class MongoConfig(uri: String, db: String)

// standard recommendation structure object:
// key: value -> productId: score
case class Recommendation(  productId: Int, score: Double )
// define recommendation list of user
case class UserRecs(userId: Int, recs: Seq[Recommendation])
// define product similarity list
case class ProductRecs( productId: Int, recs: Seq[Recommendation])

object offlineRecommender {
  // define mongo table name
  val MONGODB_RATING_COLLECTION = "Rating"

  // define recommendation table name
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

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
        rating => (rating.userId, rating.productId, rating.score)
      ).cache()

    // extract user and product data
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // core calculation
    // 1. train latent semantic model (ALS model)
    val trainData = ratingRDD.map(x=>Rating(x._1,x._2,x._3))
    // preset model parameters
    // rank: hidden feature number
    // iterations: iteration times
    // lambda: regularization factor
    val ( rank, iterations, lambda ) = ( 5, 10, 0.1 )
    val model = ALS.train( trainData, rank, iterations, lambda )

    // 2. get predict rating matrix, and get recommendation lists for users
    // user userRDD and productRDD to calculate Cartesian Product, and get an empty userProductsRDD
    val userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)

    // extract recommendation list for user from preRating
    val userRecs = preRating.filter(_.rating>0)
      .map(
        rating => ( rating.user, ( rating.product, rating.rating ) )
      )
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2>_._2)
            .take(USER_MAX_RECOMMENDATION)
            .map(x=>Recommendation(x._1, x._2)))
      }
      .toDF()
    // store to mongo
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. use feature vector to calculate product similarity
    val productFeatures = model.productFeatures.map{
      case (productId, features) => ( productId, new DoubleMatrix(features) )
    }
    // calculate cosine similarity of each other
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      //  calculate cosine similarity
      .map{
        case (a, b) =>
          val simScore = cosineSim( a._2, b._2 )
          ( a._1, ( b._1, simScore ) )
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs( productId, recs.toList.sortWith(_._2>_._2 )
            .map(x=>Recommendation(x._1, x._2)))
      }
      .toDF()
    // store to mongo
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def cosineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / ( product1.norm2() * product2.norm2() )
  }
}
