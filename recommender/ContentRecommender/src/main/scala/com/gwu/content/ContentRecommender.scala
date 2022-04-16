package com.gwu.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


case class MongoConfig(uri: String, db: String)

case class Product(
                    productId: Int,
                    name: String,
                    imageUrl: String,
                    categories: String,
                    tags: String
                  )
// standard recommendation structure object:
// key: value -> productId: score
case class Recommendation(  productId: Int, score: Double )

// define product similarity list
case class ProductRecs( productId: Int, recs: Seq[Recommendation])

object ContentRecommender {
  // define mongo table name
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // create spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // create spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    // load data and pre process
    val productTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(
        x => ( x.productId, x.name, x.tags.map(
          c => if (c == '|') ' ' else c
        ) )
      )
      .toDF("productId", "name", "tags")
      .cache()

    // use TF-IDF to extract features vector
    // 1. new a tokenizer to break the words
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // add a new column to DF called words
    val wordDataDF = tokenizer.transform(productTagsDF)

    // 2. define a HashingTF toll to calculate frequency
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF = hashingTF.transform(wordDataDF)

    // 3. define a IDF tool, to calculate TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // train an idf model
    val idfModel = idf.fit(featurizedDataDF)
    // get a DF with new column called features
    val rescaledDataDF = idfModel.transform(featurizedDataDF)
//    rescaledDataDF.show(truncate = false)

    // convert data to RRD features
    val productFeatures = rescaledDataDF.map{
      row => ( row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray )
    }
      .rdd
      .map{
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
      .filter(_._2._2>0.4)
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
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def cosineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / ( product1.norm2() * product2.norm2() )
  }
}
