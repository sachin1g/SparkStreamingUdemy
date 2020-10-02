package twitter_spark

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(10))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText().length) //.filter(status=>status.contains("ipl") || status.contains("IPL"))
    var totalChars = new AtomicLong(0L)
    var totalCount = new AtomicLong(0L)
    // Print out the first ten
    statuses.foreachRDD(rdd =>{
      if (rdd.count() > 0){
    totalCount.getAndAdd(rdd.count())
    totalChars.getAndAdd(rdd.reduce((x, y) => x + y))
    println("Total Chars:" + totalChars + " Total Tweets:" + totalCount + " Average So far :" + totalChars.get() / totalCount.get())
  }
  } )
    
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }  
}