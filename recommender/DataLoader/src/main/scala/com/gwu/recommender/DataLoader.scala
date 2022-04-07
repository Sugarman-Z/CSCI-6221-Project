package com.gwu.recommender

import com.mongodb.casbah.Imports.{MongoClientURI, MongoDBObject}
import com.mongodb.casbah.MongoClient
import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Product data
 * 3982                           productId
 * Fuhlen 富勒 M8眩光舞者时尚节能     productName
 * https://images-cn-4.ssl-image  productImageUrl
 * 外设产品/鼠标/电脑/办公            productClassification
 * 富勒/鼠标/电子产品/好用/外观漂亮     UGCTags
 */
case class Product( productId: Int, name: String, imageUrl: String, categories: String, tags: String)

/**
 * Rating data
 * 4867       userId
 * 457976     productId
 * 5.0        rating
 * 1395676800 timeStamp
 */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int)


/**
 * MongoDB connection config
 * @param uri   MongoDB connection uri
 * @param db    target database
 */
case class MongoConfig( uri: String, db: String )

object DataLoader {
  // assign data path
  val PRODUCT_DATA_PATH = "D:\\JavaProject\\CSCI6221project\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\JavaProject\\CSCI6221project\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  // define mongodb table name
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {
    // TODO: create MongoDB database
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // create spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // load data
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      // split product data with ^
      val attr = item.split("\\^")
      // convert to Product class and return
      Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim )
    } ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( item => {
      val attr = item.split(",")
      Rating( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }).toDF()

    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    storeDataInMongoDB( productDF, ratingDF )

//    spark.stop()
  }

  def storeDataInMongoDB( productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // create a mongodb connection (client)
    val mongoClient = MongoClient( MongoClientURI(mongoConfig.uri) )
    // define operation collection
    // means db.Product
    val productCollection = mongoClient( mongoConfig.db )( MONGODB_PRODUCT_COLLECTION )
    val ratingCollection = mongoClient( mongoConfig.db )( MONGODB_RATING_COLLECTION )

    // if collection exists then drop
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // store data to collection
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

    // create index for collection
    productCollection.createIndex( MongoDBObject( "productId" -> 1) )
    ratingCollection.createIndex( MongoDBObject( "productId" -> 1) )
    ratingCollection.createIndex( MongoDBObject( "userId" -> 1) )

    mongoClient.close()
  }
}
