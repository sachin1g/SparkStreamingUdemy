package session

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.util.Try

//This code tries to fetch most used words in a hotel review having rating greater than or equal to 3.
// I have used tripadvisors rating data which is freely available online
object HotelReviewSession {

  case class HotelReviewState(sessionLength:Long,reviews:List[String])

  def trackStateOfReviews(rating:Int,review:Option[String],state:State[HotelReviewState]):Option[HotelReviewState]={
  val oldState=state.getOption().getOrElse(HotelReviewState(0,List("Empty")))
    val newState=HotelReviewState(oldState.sessionLength+1,oldState.reviews:+review.getOrElse("Error"))
    state.update(newState)
    return Some(newState)
  }

  def main(args: Array[String]): Unit = {

    //create stream context
    val ssc=new StreamingContext("local[*]","groceries",Seconds(1))


    //read review data from socket
    //val hotelreviews=ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val hotelreviews=ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val stateSpec=StateSpec.function(trackStateOfReviews _).timeout(Seconds(60))
    //map reviews to case class
    val reviewRecords=hotelreviews.map(x=>mapRecordToKeyValue(x))
    //reviewRecords.print()
    val reviewState=reviewRecords.mapWithState(stateSpec)

    // And we'll take a snapshot of the current state so we can look at it.
    val stateSnapshotStream = reviewState.stateSnapshots()

    // Process each RDD from each batch as it comes in
    stateSnapshotStream.foreachRDD((rdd, time) => {

      // We'll expose the state data as SparkSQL, but you could update some external DB
      // in the real world.

      val spark = SparkSession
        .builder()
        .appName("Sessionizer")
        .getOrCreate()

      import spark.implicits._

      // Slightly different syntax here from our earlier SparkSQL example. toDF can take a list
      // of column names, and if the number of columns matches what's in your RDD, it just works
      // without having to use an intermediate case class to define your records.
      // Our RDD contains key/value pairs of IP address to SessionData objects (the output from
      // trackStateFunc), so we first split it into 3 columns using map().
      val requestsDataFrame = rdd.map(x => (x._1, x._2.sessionLength, x._2.reviews)).toDF("rating", "sessionLength", "reviews")

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("sessionData")

      // Dump out the results - you can do any SQL you want here.
      val sessionsDataFrame =
        spark.sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessionsDataFrame.show(false)

    })



    //Kickoff execution
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }


  //map records to case class
  def mapRecordToKeyValue(record:String):(Int,String)={
    val reviewSplit=record.split("\",")
    (Try(reviewSplit(1).toInt).getOrElse(0),reviewSplit(0))
  }
}
