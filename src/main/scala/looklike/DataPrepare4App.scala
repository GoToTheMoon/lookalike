package looklike

import cz.mallat.uasparser._
import org.apache.spark._
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}


trait DataPrepare4App extends Serializable {


  def rddToDF(spark: SparkSession, sc: SparkContext,cid:String):DataFrame = {
    val schema = StructType(
      Seq(
        StructField("agsid",StringType,nullable = true),
        StructField("evid", StringType,nullable = true),
        StructField("label", IntegerType,nullable = true) ,
        StructField("cid", StringType,nullable = true),
        StructField("device", StringType,nullable = true),
        StructField("channel", StringType,nullable = true)
      )
    )
    val rowRDD =sc.textFile(s"/user/tracking/tt/job/hourly/*/match01/cid_$cid").map( x => x.split('\t')).map( x => Row(x(6).trim,x(11).trim,1,x(0).trim,x(24).trim,x(23).trim))
    spark.createDataFrame(rowRDD,schema)
  }


  def rddToDFall(spark: SparkSession, sc: SparkContext,cid:String):DataFrame = {
    val schema = StructType(
      Seq(
        StructField("agsid",StringType,nullable = true),
        StructField("evid", StringType,nullable = true),
        StructField("label", IntegerType,nullable = true) ,
        StructField("cid", StringType,nullable = true),
        StructField("device", StringType,nullable = true),
        StructField("channel", StringType,nullable = true)
      )
    )
    val rowRDD =sc.textFile(s"/user/tracking/tt/job/hourly/*/match01/cid_$cid").map( x => x.split('\t')).map( x => Row(x(6).trim,x(11).trim,1,x(0).trim,x(24).trim,x(23).trim))
    spark.createDataFrame(rowRDD,schema)
  }

  //获取agsid转aguserid表
  def exchange(spark: SparkSession, sc: SparkContext,i:String):DataFrame={
    val schema=StructType(
      Seq(
        StructField("agsid",StringType,nullable = true),
        StructField("aguserid", StringType,nullable = false)
      )
    )
    val rowRDD =sc.textFile(s"/user/dauser/aguid/idmapHistory/agsid/$i").map( x => x.split('\t')).map( x => Row(x(0).trim,x(1).trim))
    spark.createDataFrame(rowRDD,schema).distinct()
  }
  //提取日志的小时数
  def dealdatahour(time:String):Int={
    val t=time.split(' ')
    val h=t(1).split(':')(0).trim
    h.toInt
  }
  val dealhours: UserDefinedFunction =udf(dealdatahour _) //注册为udf

  //ua转换为数字
  def getuanum(spark:SparkSession){
    val rawdata=spark.read.parquet(s"/user/dsp/bidder_agg/baidubn/agg/2017-12-12*/data/warehouseMidReportParquet").select("ua")
    val uadata=rawdata.limit(1000000).withColumn("uafamily",dealuas(col("ua")))
    val indexer = new StringIndexer().setInputCol("uafamily").setOutputCol("numua").fit(uadata)
    indexer.save("real_model/ua.model")}

  //ua解析
  def get_ua(ua:String):String={
    val uasParser = new UASparser(OnlineUpdater.getVendoredInputStream)
    val uadetail=uasParser.parse(ua)
    val osfamily=uadetail.getOsFamily
    val uafamily=uadetail.getUaFamily
    uafamily}
  val dealuas: UserDefinedFunction =udf(get_ua _)


  def uav(spark:SparkSession,df:DataFrame): DataFrame ={
    val uad=df.withColumn("uafamily",dealuas(col("ua"))("uafamily")).select("aguserid","uafamily")
    val uaModel = StringIndexerModel.load("real_model/ua.model")
    val indexer=uaModel.transform(uad)
    val uag=indexer.groupBy("aguserid").agg(collect_list("numua") as "ualist")
    import spark.implicits._
    val en=uag.rdd.map { line =>
      val usid = line.getString(0)
      val uanv = line.getSeq[Double](1)
      val uafv: Array[Double] = new Array[Double](50)
      for (i <- uanv.indices) uafv(uanv(i).toInt) = 1
      (usid, Vectors.dense(uafv))
    } .toDF("aguserid","uav")
    en
  }


  //收益名单连接bid日志
  def tobid(spark: SparkSession,  sc: SparkContext,i:String,cid:String):DataFrame={
    val rawdata=spark.read.parquet(s"/user/dsp/bidder_agg/*/agg/$i*/data/warehouseMidReportParquet").filter("app=false").filter("aguserid is not null")
    val sdata=rawdata.select("aguserid","site","time","ip","browser").na.drop()
    val exchanges=exchange(spark,sc,i)
    val matchdata=rddToDFall(spark,sc,cid).select("agsid","label")
    val matchdatai=exchanges.join(matchdata,Seq("agsid"),"left_outer")
    val matchd=matchdatai.select("aguserid","agsid","label")
    val nndata=matchd.filter("label is null").sample(false,0.001).join(sdata,Seq("aguserid")).select("agsid","site","time","ip","browser")
    val nn=get_features(nndata).withColumn("label",lit(0))
    val dddata1=matchd.filter("label=1").join(sdata,Seq("aguserid")).select("agsid","site","time","ip","browser")
    val pp=get_features(dddata1).withColumn("label",lit(1))
    val ddata=nn.union(pp)
    ddata.select("agsid","hourcount","sitenum","bidnum","bidhourrate","sitehourrate","label")
  }

  //获取预测数据
  def predata(spark: SparkSession, sc: SparkContext,day:String,cid:String):DataFrame={
    val rawdata=spark.read.parquet(s"/user/dsp/bidder_agg/*/agg/$day*/data/warehouseMidReportParquet").filter("app=false").filter("aguserid is not null").filter("aguserid!=''")
    val exdata=exchange(spark,sc,day)
    val idpre=exdata

    val pred=idpre.join(rawdata.select("aguserid","site","time","ip","ua"),Seq("aguserid")).select("agsid","site","time","ip","ua")

    //val pred=rawdata.select(col("channelid") as "agsid",col("site"),col("time"),col("ip"),col("ua"))
    val pfea=get_features(pred)
    pfea
  }

  def bidhourratescore(biddata:Long,hourdata:Long):Int={
    val data=biddata.toDouble/hourdata
    data match{
      case `data` if data > 0 && data <= 2 =>0
      case `data` if data > 2 && data <= 5 =>1
      case `data` if data > 5 && data <= 9 =>2
      case `data` if data > 9 && data <= 12 =>3
      case `data` if data > 12 && data <= 50 =>4
      case `data` if data > 50 =>5
    }}
  def sitehourratescore(sitedata:Long,hourdata:Long):Int={
    val data=sitedata.toDouble/hourdata
    data match{
      case `data` if data > 0 && data < 0.9 =>0
      case `data` if data >= 0.9 && data <= 1.2 =>1
      case `data` if data > 1.2 && data <= 2.0 =>2
      case `data` if data > 2 =>3
    }}
  def bidipratescore(biddata:Long,ipdata:Long):Int={
    val data=biddata.toDouble/ipdata
    data match{
      case `data` if data > 0 && data <= 12 =>0
      case `data` if data > 12 && data <= 36 =>1
      case `data` if data > 36 =>2
    }}
  def siteipratescore(sitedata:Long,ipdata:Long):Int={
    val data=sitedata.toDouble/ipdata
    data match{
      case `data` if data > 0 && data <= 1 =>0
      case `data` if data > 1 && data <= 2 =>1
      case `data` if data > 2 && data <= 3 =>2
      case `data` if data > 3 && data <= 4 =>3
      case `data` if data > 4 && data <= 7 =>4
      case `data` if data > 7 =>5
    }
  }


  def ipnum(data:Int):Double={
    data match{
      case 1=>1
      case 2=>2
      case 3=>3
      case 4=>4
      case 5=>5
      case 6=>6
      case _=>0
    }}
  def sitenum(data:Int):Double={
    data match{
      case 1 =>1
      case 2=>2
      case 3=>3
      case 4=>4
      case 5=>5
      case `data` if data > 5 && data <= 13 =>6
      case _=>0
    }}
  def bidnum(data:Int):Double={
    data match{
      case 1 =>1
      case 2=>2
      case 3=>3
      case `data` if data > 3 && data <= 8 =>4
      case _=>0
    }}
  def hournum(data:Int):Double={
    data match{
      case 1=>1
      case 2=>2
      case 3=>3
      case 4=>4
      case 5=>5
      case 6=>6
      case 7=>7
      case 8=>8
      case 9=>9
      case 10=>10
      case 11=>11
      case 12=>12
      case 13=>13
      case 14=>14
      case `data` if data > 14 && data<20=>15
      case `data` if data > 19 =>16
    }
  }
  def uanum(data:Int):Double={
    data match{
      case 1=>1
      case 2=>2
      case 3=>3
      case 4=>4
      case `data` if data > 4 && data<8=>5
      case `data` if data > 8 =>6
    }
  }

  val dealhour: UserDefinedFunction =udf(hournum _)
  val dealbid: UserDefinedFunction =udf(bidnum _)
  val dealsite: UserDefinedFunction =udf(sitenum _)
  val dealip: UserDefinedFunction =udf(ipnum _)
  val numua: UserDefinedFunction =udf(uanum _)
  val bidhourrate: UserDefinedFunction =udf(bidhourratescore _)
  val sitehourrate: UserDefinedFunction =udf(sitehourratescore _)
  val bidiprate: UserDefinedFunction =udf(bidipratescore _)
  val siteiprate: UserDefinedFunction =udf(siteipratescore _)

  //获取特征
  def get_features(ddata:DataFrame):DataFrame={

    val jdata=ddata.withColumn("hour",dealhours(col("time")))
    val fdata: DataFrame =jdata.select("agsid","hour","ip","site").cache()
    val hourdata=fdata.select("agsid","hour").groupBy("agsid","hour").count

    val hourgroup=hourdata.groupBy("agsid").count
    val hourgroupf=hourgroup.select(col("agsid"),col("count") as "hourcount")
    val sitedata=fdata.select("agsid","site").groupBy("agsid","site").count
    val sitegroup=sitedata.groupBy("agsid").count
    val sitegroupf=sitegroup.select(col("agsid"),col("count") as "sitecount",dealsite(col("count")) as "sitenum")

    val bidgroup=fdata.groupBy("agsid").count
    val bidgroupf=bidgroup.select(col("agsid"),col("count") as "bidcount",dealbid(col("count")) as "bidnum")

    val sample: DataFrame =hourgroupf.join(sitegroupf,Seq("agsid")).join(bidgroupf,Seq("agsid")).withColumn("bidhourrate",bidhourrate(col("bidcount"),col("hourcount"))).withColumn("sitehourrate",sitehourrate(col("sitecount"),col("hourcount")))
    sample
  }
}