package com.testcom.test
import org.apache.spark.{SparkConf,SparkContext}
/**
 * Created by root on 12/11/16.
 */
object wordcount {
  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setAppName("topk")
    if(args.length!=2){
      System.err.print("input < 2")
      System.exit(0)
    }
    val Array(input,output)=args
    val sc=new SparkContext(sparkConf)
    val wc=sc.textFile(input).flatMap(line=>line.split(" ")).map(w=>(w,1)).reduceByKey(_ + _).saveAsTextFile(output)
  }

}
