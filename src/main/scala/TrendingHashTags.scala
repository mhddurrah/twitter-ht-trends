
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/**
  * Created by JDuke on 10/1/17.
  */
object TrendingHashTags {
  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      System.err.println("check usage.txt")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret,
    lang, batchInterval, minThreshold, showCount) = args.take(8)

    val filters = args.takeRight(args.length - 8)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("TrendingHashTags").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))
    ssc.checkpoint("checkpoint")
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val twittsByLang = tweets.filter(_.getLang == lang)
    val hashTags = twittsByLang.map {
      _.getText
    } flatMap {
      _.split("""\s+""")
    } filter {
      _.startsWith("#")
    } map {
      println
    }

    val hashTagPairs = hashTags.map(hashtag => (hashtag, 1))

    val tagsWithCounts = hashTagPairs.updateStateByKey(
      (counts: Seq[Int], prevCount: Option[Int]) => prevCount.map { c => c + counts.sum }.orElse(Some(counts.sum))
    )

    val topHashTags = tagsWithCounts.filter { case (t, c) => c > minThreshold.toInt }

    val sortedTopHashTags = topHashTags.transform { rdd => rdd.sortBy({ case (t, c) => t }, false) }

    sortedTopHashTags.print(showCount.toInt)
    ssc.start()
    ssc.awaitTermination()

  }
}
