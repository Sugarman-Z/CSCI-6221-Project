package com.gwu.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 13543                                productId
 * 蔡康永的说话之道                        productName
 * 832,519,402                          classificationId, don't need
 * B0047Y19CI                           amazonId, don't need
 * https://images-cn-4.ssl-images       productImageUrl
 * 青春文学|文学艺术|图书音像               productClassification
 * 蔡康永|写的真好|不贵|内容不错|书          UserUGCTag
 */
case class Product(
                    productId: Int,
                    name: String,
                    imageUrl: String,
                    categories: String,
                    tags: String
                  )

/**
 * 4867   userId
 * 457976 productId
 * 5.0    rating
 * 1395676800 timestamp
 */
case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

/**
 * MongoDB连接配置
 * @param uri 连接url
 * @param db 要操作的db
 */
case class MongoConfig(uri: String, db: String)

object DataLoader {

  // define file path
  val PRODUCT_DATA_PATH = "recommender/DataLoader/src/main/resources/products_en.csv"
  val RATING_DATA_PATH = "recommender/DataLoader/src/main/resources/ratings.csv"
  // define mongo table name
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // load data
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      // split data with '^'
      var attr = item.split("\\^")
      // convert to Product class
      Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // implicit parameter
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // store to MongoDB
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
    // new a mongoClient
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // define operation table
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // if exists, delete
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // store current data
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // create index
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()

  }
}