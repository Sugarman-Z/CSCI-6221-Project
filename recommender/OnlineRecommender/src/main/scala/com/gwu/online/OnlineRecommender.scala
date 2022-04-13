package com.gwu.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


// define a connection assistance object
// connect redis and mongodb
object ConnHelper extends Serializable {
  // lazy load connection parameters
//  lazy val jedis = new Jedis("192.168.150.128", 6379)
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// standard recommendation structure object:
// key: value -> productId: score
case class Recommendation(  productId: Int, score: Double )
// define recommendation list of user
case class UserRecs(userId: Int, recs: Seq[Recommendation])
// define product similarity list
case class ProductRecs( productId: Int, recs: Seq[Recommendation])

object OnlineRecommender {
  // define some constants and tables' name
  val MONGODB_RATING_COLLECTION = "Rating"

  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    // create spark conf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // load data (similarity matrix, broadcasting)
    val simProductMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      // for later convenience, we convert the similarity data to map
      .map{ item =>
        ( item.productId, item.recs.map( x=>(x.productId, x.score) ).toMap )
      }
      .collectAsMap()
    // define broadcast variable
    val simProductsMatrixBC = sc.broadcast(simProductMatrix)

    // create kafka config parameters
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // create a DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
    )

    // convert kafkaStream to rating stream (userId|productId|score|timestamp)
    val ratingStream = kafkaStream.map{msg =>
      val attr = msg.value().split("\\|")
      ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // core algorithm, process ratingStream
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case ( userId, productId, score, timestamp ) =>
          println("rating data coming!>>>>>>>>>>>>")


          // core algorithm process
          // 1. read rating of current user from redis, and save as Array[(productId, score)]
          val userRecentlyRatings = getUserRecentlyRatings( MAX_USER_RATING_NUM, userId, ConnHelper.jedis )

          // 2. read most similar product from similarity matrix as alternative list, and save as Array[productId]
          val candidateProducts = getTopSimProducts( MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBC.value)

          // 3. calculate current recommendation priority, and get recommendation list of current user saving as Array[(productId, score)]
          val streamRecs = computeProductScore( candidateProducts, userRecentlyRatings, simProductsMatrixBC.value )

          // 4. store recommendation data to mongodb
          saveDataToMongoDB( userId, streamRecs )
      }
    }

    // start streaming
    ssc.start()
    // for debug
    println("streaming started!")
    ssc.awaitTermination()

  }

  // read num times rating from redis
  import scala.collection.JavaConversions._
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // get rating data from user rating list in redis
    // list key name is uid: USERID
    // list value is PRODUCTID: SCORE
    jedis.lrange( "userId:" + userId.toString, 0, num )
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }

  // get the similarity list of current product from broadcast similarity matrix,
  // and filter the rating which user have already rated and return sorted array
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {
    // get the similarity list of current product from broadcast similarity matrix
    val allSimProducts = simProducts(productId).toArray

    // filter the rating which user have already rated and return sorted array
    val ratingCollection = ConnHelper.mongoClient( mongoConfig.db ) ( MONGODB_RATING_COLLECTION )
    val ratingExist = ratingCollection.find( MongoDBObject("userId"->userId) )
      .toArray
      .map{ item => // only need productId
        item.get("productId").toString.toInt
      }
    // filter the all similarity product
    allSimProducts.filter( item => ! ratingExist.contains(item._1) )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x=>x._1)

  }

  // compute all the alternative products' rating
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[(Int, Double)] = {
    // define a variable length array: ArrayBuffer,
    // and use it to store every alternative products' basic score: (productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // define two map to store high score and low score counter： (productId -> count)
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // traverse all the alternative products
    // calculate the similarity between it and rated products
    for ( candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings ) {
      // read similarity between them from similarity matrix
      val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
      if ( simScore > 0.4 ) {
        // use formula to do weighted calculation
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )
        if ( userRecentlyRating._2 > 3 ) {
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // use formula to calculate recommendation priority
    // firstly, group by productId
    scores.groupBy(_._1).map{
      case (productId, scoreList) =>
        ( productId, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(increMap.getOrDefault(productId, 1)))
    }
    // return recommendation list, and sorted by rating
      .toArray
      .sortWith(_._2>_._2)
  }

  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Double = {
    simProducts.get(product1) match {
      case Some(sims) => sims.get(product2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // diy log
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  // write into mongodb
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
    streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
      "recs" -> streamRecs.map(x=>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
  }
}
