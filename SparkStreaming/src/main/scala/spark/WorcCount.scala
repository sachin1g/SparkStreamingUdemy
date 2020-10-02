package spark

import org.apache.spark.SparkConf

object WorcCount  extends  App {

  val sc=new org.apache.spark.SparkContext(new SparkConf().setMaster("local[*]").setAppName("wordcount"))
  val x="this is a line is a line for word count.This program will count the words of this line"
  val rd=sc.parallelize(x.split(" "))
  rd.map(word=>(word,1)).reduceByKey((x,y)=>x+y).collect().foreach(println)
}
