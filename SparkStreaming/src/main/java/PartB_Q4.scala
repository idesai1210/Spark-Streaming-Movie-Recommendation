import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf


object PartB_Q4 {
  def main(args: Array[String]) {
    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "test", "20")
    val sparkConf = new SparkConf().setAppName("HW3")
                      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    
     def stem(word: String) ={
             if(word.toLowerCase().endsWith("ness")) 
                    word.replace(word, word.substring(0, word.length()-3))
             else if(word.toLowerCase().endsWith("tion")) 
                    word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("sion")) 
                    word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("iness")) 
                    word.replace(word, word.substring(0, word.length()-5)+"y") 
             else if(word.toLowerCase().endsWith("er")) 
                    word.replace(word, word.substring(0, word.length()-2))
             else if(word.toLowerCase().endsWith("or")) 
                    word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("ily")) 
                     word.replace(word, word.substring(0, word.length()-3)+"y") 
             else if(word.toLowerCase().endsWith("ily")) 
                     word.replace(word, word.substring(0, word.length()-3)+"y") 
             else if(word.toLowerCase().endsWith("ist")) 
                     word.replace(word, word.substring(0, word.length()-3))
             else if(word.toLowerCase().endsWith("ize")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("en")) 
                    word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("ful")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("full")) 
                     word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("ical")) 
                     word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("ic")) 
                     word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("sses")) 
                    word.replace(word, word.substring(0, word.length()-4)+"ss") 
             else if(word.toLowerCase().endsWith("ies")) 
                     word.replace(word, word.substring(0, word.length()-3)+"i") 
             else if(word.toLowerCase().endsWith("ss")) 
                     word.replace(word, word.substring(0, word.length()-2)+"ss") 
             else if(word.toLowerCase().endsWith("s")) 
                     word.replace(word, word.substring(0, word.length()-1))
             else if(word.toLowerCase().endsWith("eed")) 
                     word.replace(word, word.substring(0, word.length()-3)+"ed") 
             else if(word.toLowerCase().endsWith("ed")) 
                     word.replace(word, word.substring(0, word.length()-2))
             else if(word.toLowerCase().endsWith("ing")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("ly")) 
                     word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("es")) 
                    word.replace(word, word.substring(0, word.length()-2))
      
             else 
                 word
           
      } 
    
    
    
      val stopWords1 = "(a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your)".r
      val stopWordsArray = stopWords1.split(",")    
      val stopWords = "(\\ba\\b|\\bable\\b|\\babout\\b|\\bacross\\b|\\bafter\\b|\\ball\\b|\\balmost\\b|\\balso\\b|\\bam\\b|\\bamong\\b|\\ban\\b|\\band\\b|\\bany\\b|\\bare\\b|\\bas\\b|\\bat\\b|\\bbe\\b|\\bbecause\\b|\\bbeen\\b|\\bbut\\b|\\bby\\b|\\bcan\\b|\\bcannot\\b|\\bcould\\b|\\bdear\\b|\\bdid\\b|\\bdo\\b|\\bdoes\\b|\\beither\\b|\\belse\\b|\\bever\\b|\\bevery\\b|\\bfor\\b|\\bfrom\\b|\\bget\\b|\\bgot\\b|\\bhad\\b|\\bhas\\b|\\bhave\\b|\\bhe\\b|\\bher\\b|\\bhers\\b|\\bhim\\b|\\bhis\\b|\\bhow\\b|\\bhowever\\b|\\bi\\b|\\bif\\b|\\bin\\b|\\binto\\b|\\bis\\b|\\bit\\b|\\bits\\b|\\bjust\\b|\\bleast\\b|\\blet\\b|\\blike\\b|\\blikely\\b|\\bmay\\b|\\bme\\b|\\bmight\\b|\\bmost\\b|\\bmust\\b|\\bmy\\b|\\bneither\\b|\\bno\\b|\\bnor\\b|\\bnot\\b|\\bof\\b|\\boff\\b|\\boften\\b|\\bon\\b|\\bonly\\b|\\bor\\b|\\bother\\b|\\bour\\b|\\bown\\b|\\brather\\b|\\bsaid\\b|\\bsay\\b|\\bsays\\b|\\bshe\\b|\\bshould\\b|\\bsince\\b|\\bso\\b|\\bsome\\b|\\bthan\\b|\\bthat\\b|\\bthe\\b|\\btheir\\b|\\bthem\\b|\\bthen\\b|\\bthere\\b|\\bthese\\b|\\bthey\\b|\\bthis\\b|\\btis\\b|\\bto\\b|\\btoo\\b|\\btwas\\b|\\bus\\b|\\bwants\\b|\\bwas\\b|\\bwe\\b|\\bwere\\b|\\bwhat\\b|\\bwhen\\b|\\bwhere\\b|\\bwhich\\b|\\bwhile\\b|\\bwho\\b|\\bwhom\\b|\\bwhy\\b|\\bwill\\b|\\bwith\\b|\\bwould\\b|\\byet\\b|\\byou\\b|\\byour\\b)".r
    
      val words = lines.map{_.split('.').map{ substrings =>substrings.trim.split(' ')
        .map{
              _.replaceAll("""[\?\.\!",\- \s]+""", "").toLowerCase()}
              .map(a=>stopWords.replaceAllIn(a,"be"))
              .map(v=>{stem(v)}).sliding(2)
         }.flatMap{identity}
         .map{_.mkString(" ")}
         .groupBy{identity}
         .mapValues{_.size}
      }
      
    val reducedCount = words.flatMap{identity}.reduceByKey(_+_)    

    val output= reducedCount.filter(a=>a._2 >= 2)
    output.print()
      
    
    ssc.start()
    ssc.awaitTermination()
    
    
    
  }
  
  
}