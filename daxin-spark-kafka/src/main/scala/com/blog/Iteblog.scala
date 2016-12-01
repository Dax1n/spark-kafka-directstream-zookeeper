package com.iteblog

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

/**
  * Created by iteblog
  *
  * blog: http://wwww.iteblog.com
  *
  */
object Iteblog {


  val groupID = "iteblog"

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "http://www.iteblog.com:9092",
    "group.id" -> "iteblog")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Test")
    sparkConf.set("spark.kryo.registrator", "utils.CpcKryoSerializer")
    val sc = new SparkContext(sparkConf)


    val ssc = new StreamingContext(sc, Seconds(2))
    val topicsSet = Set("iteblog")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages.foreachRDD(rdd => {
      // 把RDD转成HasOffsetRanges类型（KafkaRDD extends HasOffsetRanges）
      // OffsetRange 说明：Represents a range of offsets from a single Kafka TopicAndPartition.
      // OffsetRange 说明： Instances of this class can be created with `OffsetRange.create()`.
      val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //     offsetRanges 的实现代码（KafkaRDD中）：tp：TopicAndPartition,fo:fromOffset
      //      val offsetRanges = fromOffsets.map { case (tp, fo) =>
      //        val uo = untilOffsets(tp)
      //        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
      //      }.toArray

      val kc = new KafkaCluster(kafkaParams)
      for (offsets <- offsetsList) {
        //TopicAndPartition 主构造参数第一个是topic，第二个是 partition id
        val topicAndPartition = TopicAndPartition("iteblog", offsets.partition) //offsets.partition表示的是Kafka partition id
        val o = kc.setConsumerOffsets(groupID, Map((topicAndPartition, offsets.untilOffset)))
        if (o.isLeft) {
          println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}


//case class TopicAndPartition(topic: String, partitionId: Int)//Convenience case class since (topic, partition) pairs are ubiquitous.
//class Partition(val topic: String, val partitionId: Int, time: Time, replicaManager: ReplicaManager)
///**
//  *
//  * @param topic Kafka topic name
//  * @param partition Kafka partition id
//  * @param fromOffset inclusive starting offset
//  * @param untilOffset exclusive ending offset
//  */
//final class OffsetRange private(val topic: String, val partition: Int, val fromOffset: Long, val untilOffset: Long) extends Serializable