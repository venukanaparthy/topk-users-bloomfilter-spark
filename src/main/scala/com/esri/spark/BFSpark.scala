package com.esri.spark

import java.io.PrintWriter
import java.io.File

import com.twitter.algebird._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast

/*
 *  Uses Twitter's algebird BloomFilterMoniod to find top k spending users for marketing
 */
object BFSpark {
  
  /*
   * filter header
   */
  def isHeader(line: String): Boolean = line.contains("userid")
  /*
   * validate phone
   */
  def isEmail(item: String): Boolean = item.contains("@")
  /*
   * validate phone
   */
  def hasPhoneNum(item: String): Boolean =  item.contains("-") && item.contains("(")
  
  /*
   * filter transactions using users bloom filter
   */
  def transactionsBloomFilter( usersBF:Broadcast[BF], userTransactionId : String):Boolean = {
    println(userTransactionId + ", set membership : " + usersBF.value.contains(userTransactionId).isTrue)
     
    usersBF.value.contains(userTransactionId).isTrue
  }
   
  /*
   * do not call list filter
   */
  def donotcallFilter(donotcallBC:Broadcast[Array[String]], phoneListStr:String ) : Boolean = {
     if (phoneListStr != null){
       //println(phoneListStr)      
       var phones = phoneListStr.split(",")
       return donotcallBC.value.intersect(phones).size == 0
     }
     return false;
  }
      
  def main(args: Array[String]): Unit = {
     if (args.length < 3) {
      System.err.println("Usage BFSpark <input csv file>")
      //bin\spark-submit --driver-memory 6g --executor-memory 6g 
      // --class com.esri.spark.BFSpark target\topk-users-bf-spark-0.1.jar users.txt donotcall.txt transactions.txt
      return
    }
    val conf = new SparkConf()
      .setAppName("SelectTopKUsers")
      .set("spark.executor.memory", "6g")      
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val usersFile = args(0);
    val donotcallFile = args(1)
    val transactionsFile = args(2)    
    
    val usersRaw = sc.textFile(usersFile) 
    val usersRDD = usersRaw.filter(!isHeader(_))    
    //usersRDD.take(5).foreach(println)
    
    //broadcast donot call list
    val donotcallRaw = sc.textFile(donotcallFile)
    val donotcallList = donotcallRaw.distinct().collect();
    //donotcallList.foreach(println)
    val donotcallBC = sc.broadcast[Array[String]](donotcallList)
    
    
    //user data format: userid;name,email,phone1,phone2
    //filter user dataset for valid data
    //remove invalid email, phone numbers
    //some users might have more than one phone numbers listed
    val users = usersRDD.filter(hasPhoneNum(_))
                         .map(_.split(";"))
                         .map(_.filter(!isEmail(_)))
                         .map{case tokens => (tokens(0), tokens(1), tokens(2))}  // emit(id, name, phone)
                         .filter {case (a, b, c) => donotcallFilter(donotcallBC, c)}
                         .map { case (a, b, c) => (a, (b,c))}
                         .cache()
                         
    println("Number of user records " + users.count)
    println("Typical user record:" + users.first)
    //users.take(5).foreach(println)
        
    //create bloom filter for small dataset (users dataset)
    //users dataset is smaller than transactions
    
    val NUM_HASHES = 3
    val WIDTH = 8192
    val SEED = 1
    val bf = BloomFilterMonoid(NUM_HASHES, WIDTH, SEED);
    
    val usersBFRDD = users.map { case (userid, (name, phones)) =>                        
                                  bf.create(userid)
                                }
    
    val usersBF = usersBFRDD.reduce{ (a:BF, b:BF) =>  a ++ b}
    //broadcast users bloomfilter
    val usersBFBC = sc.broadcast[BF](usersBF)   
 
   
    val transactionsRaw = sc.textFile(transactionsFile)
    //println("User transactions")
    //transactionsRaw.take(5).foreach(println)
    val YEAR = "2015"
    val transactionsRDD = transactionsRaw.map(_.replace("$", ""))  
                                     .map(_.replace("-", ";")) //replace 2015-09-05 with 2015;09;05 to split and remove month/date
                                     .map(_.split(";"))  // convert to Array(815581247, 144.82, 2015, 09, 05)                                      
                                     .filter{ x => x(2) == YEAR} // filter by 2015 entries
                                     .map{arr => (arr(0), (arr(1).toDouble))} // keep customerID and amount
                                     .reduceByKey(_ + _)  
                                     .filter { case(id, dollar) => transactionsBloomFilter(usersBFBC, id)}
                                     .join(users)
                                     .map {case (userid, (dollar, (name, phones))) => UserTransaction (userid, name, phones, dollar)}
                                     .cache()    

    println("Number of transaction records " + transactionsRDD.count)
    println("Typical merged transaction record:" + transactionsRDD.first)
    
    val SEP = ";"
    val NUM_OF_USER = 1000
    val topUsers = transactionsRDD
      .takeOrdered(NUM_OF_USER)(Ordering[Double].reverse.on(x=>x.amount))
      .map(x => x.id + SEP + x.name + SEP + x.phone + SEP + "$" + "%.2f".format(x.amount))

    val pw = new PrintWriter(new File("./data/top_users.txt" ))
    for (elem <- topUsers) {
      pw.write(elem)
      pw.write("\n")
    }
    pw.close
                                                                           
  }
}

case class UserTransaction(id:String, name: String, phone: String, amount:Double)
