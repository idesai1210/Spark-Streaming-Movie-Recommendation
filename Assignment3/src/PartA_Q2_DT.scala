import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._

object PartA_Q2_DT {
  
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
    
    val dTreeSplits = parsedGlassData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainData = dTreeSplits(0)
    val testData = dTreeSplits(1)
    
    
    val num = 8
   
    val modelD = 5
    val modelB = 32
    
    val features = Map[Int, Int]()
    val impurity = "gini"
    
    val mDTree = DecisionTree.trainClassifier(trainData, num, features,
      impurity, modelD, modelB)
    
    val lbl = testData.map { pnt =>
    val prediction = mDTree.predict(pnt.features)
    (pnt.label, prediction)
    }
    val DTreeAccuracy =  1.0 *lbl.filter(r => r._1 == r._2).count.toDouble / testData.count
    
    println("Decision Tree has the accuracy of : " + (DTreeAccuracy*100)+"%")
            
  }
}