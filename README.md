# topk-users-bloomfilter-spark
TOP K users using bloom filter in Spark

Uses Twitter's algebird BloomFilterMoniod to find top k spending users for marketing

To build: mvn clean package

To run: bin\spark-submit --driver-memory 6g --executor-memory 6g 
        	 --class com.esri.spark.BFSpark target\topk-users-bf-spark-0.1.jar users.txt donotcall.txt transactions.txt
