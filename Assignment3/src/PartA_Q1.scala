import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object PartA_Q1 {
  def main(args : Array[String]){
   
      val config = new SparkConf()
                  .setAppName("Mutual Friends")
                  .setMaster("local[4]")
      
      val sparkContext = new SparkContext(config)

      val itemUserData = sparkContext.textFile("hdfs://localhost:9000/input/itemusermat")
      
      val meaningFullData = itemUserData.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()
      
     
      val kMeansClusters = 10
      val kMeansIterations = 20
      val traininClusters = KMeans.train(meaningFullData, kMeansClusters, kMeansIterations)
      
      val predictionData = itemUserData.map{ str =>	
      val item = str.split(' ')	
      (item(0),traininClusters.predict(Vectors.dense(item.tail.map(_.toDouble))))
      }
      
      val movieData = sparkContext.textFile("hdfs://localhost:9000/input/movies.dat")
      val  parsedMoviesData = movieData.map{ line=>	
      val item = line.split("::")
      (item(0),(item(1)+" , "+item(2)))
      }
      
      val combinedData = predictionData.join(parsedMoviesData)
      
      val shuffledData= combinedData.map(p=>(p._2._1,(p._1,p._2._2)))
      
      val groupedData = shuffledData.groupByKey()
      
      val result = groupedData.map(p=>(p._1,p._2.toList))
      
      val finalAnswer = result.map(p=>(p._1,p._2.take(5)))
      
      finalAnswer.collect
      
      val answer = finalAnswer.sortBy(_._1)
      
      println("Cluster ID , Top 5 Movies")
      
      finalAnswer.foreach(p=>println("Cluster ID - "+ p._1+" --> "+p._2.mkString(":::")))
    
  }
  
        
}