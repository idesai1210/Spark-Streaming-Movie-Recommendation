import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object PartA_Q2_NB {
  def main(args : Array[String]){
     val config = new SparkConf()
                      .setAppName("HW3")
                      .setMaster("local[4]")
          
    val sparkContext = new SparkContext(config)
    
    val glassData = sparkContext.textFile("hdfs://localhost:9000/input/glass.data")
    val parsedGlassData = glassData.map { str =>
	  val items = str.split(',')
  	LabeledPoint(items(10).toDouble, Vectors.dense(items(0).toDouble,items(1).toDouble,items(2).toDouble,items(3).toDouble,items(4).toDouble,items(5).toDouble,items(6).toDouble,items(7).toDouble,items(8).toDouble,items(9).toDouble))
    }

    val NBayesSplits = parsedGlassData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainData = NBayesSplits(0)
    val testData = NBayesSplits(1)

    val mNaiveBayes = NaiveBayes.train(trainData, lambda = 1.0)

    val lbl = testData.map(p => (mNaiveBayes.predict(p.features), p.label))
    val NBayesAccuracy = 1.0 * lbl.filter(x => x._1 == x._2).count() / testData.count()

    println("Naive Bayes has the accuracy of : " + (NBayesAccuracy*100)+"%")
    
  }  
}