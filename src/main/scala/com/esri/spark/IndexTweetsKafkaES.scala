package com.esri.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Time


object IndexTweetsKafkaES {
  def main(args: Array[String]): Unit = {
     /*if (args.length < 3) {
      System.err.println("Usage IndexTweetsKafkaES <input csv file>")
      //bin\spark-submit --driver-memory 6g --executor-memory 6g 
      // --class com.esri.spark.IndexTweetsKafkaES target\topk-users-bf-spark-0.1.jar users.txt donotcall.txt transactions.txt
      return
    }*/
     
    val conf = new SparkConf()
      .setAppName("IndexTweetsKafkaES")
      .set("spark.executor.memory", "6g")            
    val sc = new SparkContext(conf)
    
     def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sc, Seconds(1))
      println(s"Creating new StreamingContext $newSsc")

      newSsc
    }
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)
    
    val brokers = "localhost:9092"
    val topics = Set("tweets")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val twiterKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)    
    twiterKafkaStream.foreachRDD {
      (message:RDD[(String, String)], batchTime: Time) => {
        message.cache()      
        val tweet = message.map(_._2)
	      println(tweet);
      	message.unpersist()
      }
    }
      
    ssc.start();
    ssc.awaitTermination();
  }
}