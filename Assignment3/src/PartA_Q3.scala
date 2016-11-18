import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import breeze.linalg._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object PartA_Q3 {
   def main(args : Array[String]){
     
    
    val config = new SparkConf()
                      .setAppName("HW3")
                      .setMaster("local[4]")
          
    val sparkContext = new SparkContext(config)
    
    
    val ratingsData = sparkContext.textFile("hdfs://localhost:9000/input/ratings.dat")
   
    
//    val parsedRatingsData = ratingsData.map{ str =>
//      val items = str.split("::")
//      Rating(items(4).toDouble, Vectors.dense(items(0).toDouble,items(1).toDouble,items(2).toDouble,items(3).toDouble))
//      
//    }
//    val parsedRatingsData = ratingsData.map(_.split("::") match { case Array(user, item, rate) =>
//      Rating(user.toInt, item.toInt, rate.toDouble)
//    })

    val parsedRatingsData = ratingsData.map { str =>
      val items = str.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (Rating(items(0).toInt, items(1).toInt, items(2).toDouble))
    }
    
    val ALSSplits = parsedRatingsData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainData = ALSSplits(0)
    val testData = ALSSplits(1)
    
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val ALSmodel = ALS.train(trainData, rank, numIterations, 0.01)
    
    
   // Evaluate the model on rating data
    val modelEvaluation = testData.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictedModel =
      ALSmodel.predict(modelEvaluation).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = testData.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictedModel)
    val ALSAccuracy = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val error = (r1 - r2)
      error * error
    }.mean()
    println("ALS has the accuracy of : " + ALSAccuracy*100)
     
   }
}