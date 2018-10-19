package looklike

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, _}
import util.kafuka

object Using extends DataPrepare4App {

  def get_train_file(spark: SparkSession,  sc: SparkContext,i:String,cid:String): Unit ={
    val traindata=tobid(spark,sc,i,cid)
    traindata.write.parquet(s"$cid"+s"modeldata$i")
  }

  def train(spark: SparkSession,i:String,cid:String): Unit ={
    val trista=spark.read.parquet(s"$cid"+s"modeldata$i")

    val cat1Index: StringIndexer = new StringIndexer().setInputCol("hourcount").setOutputCol("indexedCat1").setHandleInvalid("skip")
    val cat1Encoder: OneHotEncoder = new OneHotEncoder().setInputCol("indexedCat1").setOutputCol("hourv")
    val cat2Index = new StringIndexer().setInputCol("sitenum").setOutputCol("indexedCat2").setHandleInvalid("skip")
    val cat2Encoder = new OneHotEncoder().setInputCol("indexedCat2").setOutputCol("provicev")
    val cat3Index = new StringIndexer().setInputCol("bidnum").setOutputCol("indexedCat3").setHandleInvalid("skip")
    val cat3Encoder = new OneHotEncoder().setInputCol("indexedCat3").setOutputCol("channelv")
    val cat4Index = new StringIndexer().setInputCol("bidhourrate").setOutputCol("indexedCat4").setHandleInvalid("skip")
    val cat4Encoder = new OneHotEncoder().setInputCol("indexedCat4").setOutputCol("dtv")
    val cat5Index = new StringIndexer().setInputCol("sitehourrate").setOutputCol("indexedCat5").setHandleInvalid("skip")
    val cat5Encoder = new OneHotEncoder().setInputCol("indexedCat5").setOutputCol("pfv")

    val va = new VectorAssembler().setInputCols(Array("channelv", "hourv", "provicev", "dtv", "pfv")).setOutputCol("features")
    val pipelineStage = Array(cat1Index, cat2Index, cat3Index, cat4Index, cat5Index, cat1Encoder, cat2Encoder, cat3Encoder, cat4Encoder, cat5Encoder, va)
    val pipline = new Pipeline().setStages(pipelineStage)
    val pModle = pipline.fit(trista)

    val trdata=pModle.transform(trista)
    pModle.save(s"pmmodel$cid$i")
    //
    val Bayesmodel=new NaiveBayes().setFeaturesCol("features").setLabelCol("label").setModelType("bernoulli")
    val bmodel1=Bayesmodel.fit(trdata)
    bmodel1.save(s"bymodel$cid$i")
  }

  def ds(s:String):String={
    val ss=s.substring(1,s.length-1)
    ss
  }

  def predict(spark: SparkSession,  sc: SparkContext,i:String,cid:String): Unit ={
    val pmodel=PipelineModel.load(s"pmmodel$cid$i")
    val bmodel=NaiveBayesModel.load(s"bymodel$cid$i")
    val predictdata=predata(spark,sc,i,cid)
    val pdt=pmodel.transform(predictdata)
    val score=bmodel.transform(pdt)
    def dealp0(v: DenseVector): Double = {
      val sv=v(0)
      sv}
    val getp0=udf(dealp0 _)
    val time = new Date().getTime
    val format = new SimpleDateFormat("yy-MM-dd HH:mm")
    val ti=format.format(time)
    val tf=ti.replaceAll("-| |:","")
    val sd=score.withColumn("p0",getp0(col("probability"))).select("agsid","p0").sort("p0")
    val ss=sd.limit(600000)
    def getlabel(p:Double): String ={
      s"{X:MO!Y:LK!Z:01!C:$cid!V:1!T:$tf}"
    }
    val getlabell=udf(getlabel _)
    ss.withColumn("lab",getlabell(col("p0"))).select("agsid","lab").rdd.map(line=>line.toSeq.mkString(",")).saveAsTextFile(s"$cid"+s"label$i")
  }

  def tokaFuka(sc:SparkContext,i:String,cid:String): Unit ={
    val path=s"$cid"+s"label$i"
    val data=sc.textFile(path).map(_.split(",")).filter(_.length==2).map(x=>(x(0),x(1)))

    kafuka.deviceId2kafka(data,5)
  }
}