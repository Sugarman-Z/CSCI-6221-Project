package com.gwu.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


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
// define product similarity list
case class ProductRecs( productId: Int, recs: Seq[Recommendation])

object ItemCFRecommender {
  // define constant and table name
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data and convert to DF
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(
        x => ( x.userId, x.productId, x.score )
      )
      .toDF("userId", "productId", "score")
      .cache()

    // core algorithm (calculate co-occurrence similarity and get product similarity)
    // 1. Count the number of ratings for each item
    // group by productId
    val productRatingCountDF = ratingDF.groupBy("productId").count()
    // add a column called count to original rating table
    val ratingWithCountDF = ratingDF.join(productRatingCountDF, "productId")

    // get the two products which are rated by one user
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2")
      .select("userId","product1","count1","product2","count2")
    // create a temporary table for selection
    joinedDF.createOrReplaceTempView("joined")

    // group by product1 and product2 and count userId
    val cooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
      """.stripMargin
    ).cache()

    // extract needed data and return as ( productId1, ( productId2, score ) )
    val simDF = cooccurrenceDF.map{
      row =>
        val coocSim = cooccurrenceSim( row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2") )
        ( row.getInt(0), ( row.getInt(1), coocSim ) )
    }
      .rdd
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs( productId, recs.toList
            .filter(x=>x._1 != productId)
            .sortWith(_._2>_._2)
            .take(MAX_RECOMMENDATION)
            .map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()

    // store to mongodb
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // use formula to calculate cooccerrenceSim
  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long): Double ={
    coCount / math.sqrt( count1 * count2 )
  }
}
