package streaming_practice

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try
//This code tries to fetch most used words in a hotel review having rating greater than or equal to 3.I have used tripadvisors rating data which is freely available online
object Hotelreview {
  def main(args: Array[String]): Unit = {

    //create stream context
    val ssc=new StreamingContext("local[*]","groceries",Seconds(1))


    //read review data from socket
    val hotelreviews=ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    //map reviews to case class
    val reviewRecords=hotelreviews.map(x=>mapRecordToClass(x))
    //reviewRecords.print()


    reviewRecords.transform(rd=>rd.filter(record=>record.rating>=3). //filter records with rating greater than or equal to 3
      flatMap(record=>record.review.split("[., ]")).map(x=>(x,1))) //split review into words and mapp each word with 1 for word count
      .reduceByKeyAndWindow(_+_,_-_,Seconds(6),Seconds(2)) // do addition of counts on window of 6 seconds with slide interval of 2 seconds
      .transform(rd=>rd.sortBy(_._2,false)).print() //sort in descending order and print

    //Kickoff execution
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }


  //map records to case class
  def mapRecordToClass(record:String):reviewRecord={
    val reviewSplit=record.split("\",")
    reviewRecord(reviewSplit(0),Try(reviewSplit(1).toInt).getOrElse(0))
  }
}

case class reviewRecord(review:String,rating:Int)
