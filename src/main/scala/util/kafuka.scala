package util

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object kafuka {

  val KAFKA_BATCH_SIZE = 5000
  val channelId = "-1" // 发往所有集群的redis
  val output_topic="topic_tagdata"
  //val output_topic="topic_test"
  def createKafkaProducer() ={
    val props = new Properties()
    val brokers="l-kafka1.prod.qd1.corp.agrant.cn:9092,l-kafka2.prod.qd1.corp.agrant.cn:9092,l-kafka3.prod.qd1.corp.agrant.cn:9092"
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)
    producer
  }
  def collectAsBatch(arrBuffer:ArrayBuffer[ArrayBuffer[KeyedMessage[String,String]]],message:String,topic:String,batchSize:Int)={
    val keyedMessage = new KeyedMessage[String, String](topic, message);
    if(arrBuffer.last.length < batchSize){arrBuffer.last += keyedMessage;arrBuffer;}
    else arrBuffer += {new ArrayBuffer[KeyedMessage[String, String]](batchSize) += keyedMessage}
  }
  def toKafka(data:RDD[(String,String)]): Unit ={
    //**警告**警告**警告**警告**警告**警告**警告**警告: 实时标签的tag中的","必须转换成"!"
    data.foreachPartition(p => {
      val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time = inputFormat.format(new Date()) //暂时用当前时间
      val producer = createKafkaProducer()
      p.foldLeft(ArrayBuffer(new ArrayBuffer[KeyedMessage[String, String]](KAFKA_BATCH_SIZE)))((x, y) => {
        val (uid, tag) = y;
        val tagField = tag.replace(",","!") + s"|~|$time"// 实时标签的tag中的","必须转换成"!"
        val message = Array(channelId, "", uid, tagField).mkString("\t")
        collectAsBatch(x, message, output_topic, KAFKA_BATCH_SIZE)
      }).foreach(x => {
        producer.send(x: _*);
      })
      producer.close();
    })
  }

  def deviceId2kafka(data: RDD[(String, String)],batchNum:Int): Unit = {
    val array = new ArrayBuffer[Double]()
    for (i <- 1 to batchNum) {
      array += 1.0/batchNum
    }
    val list = data.randomSplit(array.toArray)
    list.foreach(p => {
      toKafka(p)
      println("count:" + p.count())
      Thread.sleep(60000)
    })
  }
  def agsid2kafka(data:RDD[(String,String)]): Unit ={
    toKafka(data)
    println("count:"+data.count())
  }
}