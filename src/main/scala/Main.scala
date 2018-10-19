import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * Created by wenbin.lu on 2017-11-29.
  * lookalike 项目入口
  **/
object Main {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("bid")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new SQLContext(sc)
    Logger.getRootLogger.setLevel(Level.ERROR)


    if (args.length>2){
      val model=args(2)
      model match {
        case "trainfile"=>looklike.Using.get_train_file(spark,sc,args(0),args(1))
        case "model"=> looklike.Using.train(spark,args(0),args(1))
        case "predict"=>looklike.Using.predict(spark,sc,args(0),args(1))
        case "tokafuka"=>looklike.Using.tokaFuka(sc,args(0),args(1))
      }
    }else {
      looklike.Using.get_train_file(spark, sc, args(0),args(1))
      looklike.Using.train(spark, args(0),args(1))
      looklike.Using.predict(spark, sc, args(0),args(1))
      looklike.Using.tokaFuka(sc, args(0),args(1))
    }
  }
}