
package log_parser
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import java.util.regex.Pattern
import java.util.regex.Matcher

import twitter_spark.Utilities._

/** Maintains top URL's visited over a 5 minute window, from a stream
 *  of Apache access logs on port 9999.
 */
object LogParser {
 
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //for referer
    /*
    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(9)})
    
    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    */


    //forIP


    // Extract the request field from each log line
    /*
    val ips = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(1)})


    // Reduce by URL over a 5-minute window sliding every second
    val  ipcounts= ips.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
*/
    //till here forIP

    //for status



    // Extract the request field from each log line
    val statuses = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(6) else "Error"})
val statusMapped=statuses.map(status=>{
  val code=scala.util.Try(status.toInt) getOrElse(0)
  if(code >= 200 && code <= 300){
    "Success"
  }else if (code  >= 500 && code <= 600 ){
    "Failure"
  }else{
    "Other"
  }
})
    // Reduce by URL over a 5-minute window sliding every second
    val  statusCounts= statusMapped.countByValueAndWindow( Seconds(1800), Seconds(5))

    var successCount=0L
    var failureCount=0L
    //end status
    // Sort and print the results
    val sortedResults = statusCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    sortedResults.foreachRDD(rdd=>{
      val rdc=rdd.collect()
      successCount=successCount+scala.util.Try(rdc.filter(x=>x._1=="Success")(0)._2.toInt).getOrElse(0)
      failureCount=failureCount+scala.util.Try(rdc.filter(x=>x._1=="Failure")(0)._2.toInt).getOrElse(0)
      println("Success Count:"+successCount+" Failure Count:"+failureCount)
    })
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

