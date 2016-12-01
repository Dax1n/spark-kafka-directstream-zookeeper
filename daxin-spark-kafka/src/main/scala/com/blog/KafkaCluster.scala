package org.apache.spark.streaming.kafka

//使用org.apache.spark.streaming.kafka的原因：  private[spark] object SimpleConsumerConfig限制只在spark包中使用！

import kafka.api.{OffsetCommitRequest, OffsetCommitResponse}
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.SimpleConsumerConfig

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

class KafkaCluster(val kafkaParams: Map[String, String]) extends Serializable {
   type Err = ArrayBuffer[Throwable]

   @transient private var _config: SimpleConsumerConfig = null

   def config: SimpleConsumerConfig = this.synchronized {
     if (_config == null) {
       //SimpleConsumerConfig的apply方法部分代码：
       //val brokers = kafkaParams.get("metadata.broker.list").orElse(kafkaParams.get("bootstrap.servers"))
       //所以kafkaParams必须包含key=metadata.broker.list或者bootstrap.servers对应的Value
       _config = SimpleConsumerConfig(kafkaParams)
     }
     _config
   }

  /**
    *
    * @param groupId: String
    * @param offsets: Map[TopicAndPartition, Long]
    * @return
    */
   def setConsumerOffsets(groupId: String,
                          offsets: Map[TopicAndPartition, Long]
                           ): Either[Err, Map[TopicAndPartition, Short]] = {
     setConsumerOffsetMetadata(groupId, offsets.map { kv =>
       //kv._2 偏移
       kv._1 -> OffsetMetadataAndError(kv._2)//返回一个Map[TopicAndPartition, OffsetMetadataAndError]的Map
     })
   }

   def setConsumerOffsetMetadata(groupId: String,
                                 metadata: Map[TopicAndPartition, OffsetMetadataAndError]
                                  ): Either[Err, Map[TopicAndPartition, Short]] = {

     // 更新到zookeeper偏移的放到result中，然后每一次和待处理的集合做diff运算，计算没有更新的tp在更新
     var result = Map[TopicAndPartition, Short]()
     val req = OffsetCommitRequest(groupId, metadata)
     val errs = new Err
     val topicAndPartitions:Set[TopicAndPartition] = metadata.keySet

     //withBrokers是一个柯里化函数，config.seedBrokers获取Broker的ip地址和端口号的元组的数组即：Array[(ip, port)]
     //Random.shuffle(config.seedBrokers) 打乱数组顺序  //consumer相当于匿名函数的参数
     withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
       val resp:OffsetCommitResponse = consumer.commitOffsets(req)//提交 “提交偏移”
       val respMap:Map[TopicAndPartition, Short] = resp.requestInfo
       //diff Computes the difference of this set and another set.
       val needed:Set[TopicAndPartition] = topicAndPartitions.diff(result.keySet)
       needed.foreach { tp: TopicAndPartition =>
         respMap.get(tp).foreach { err: Short =>

           //object ErrorMapping 代码和异常的一个双向映射
           // object ErrorMapping： A bi-directional mapping between error codes and exceptions

           if (err == ErrorMapping.NoError) {
             //更新到zookeeper偏移的放到result中
             result += tp -> err
           } else {
             errs.append(ErrorMapping.exceptionFor(err))
           }
         }
       }
       if (result.keys.size == topicAndPartitions.size) {
         return Right(result)
       }
     }
     val missing = topicAndPartitions.diff(result.keySet)
     errs.append(new SparkException(s"Couldn't set offsets for ${missing}"))
     Left(errs)
   }

  /**
    *
    * @param brokers  (ip, port)
    * @param errs
    * @param fn  consumer => Either[Err, Map[TopicAndPartition, Short ] ] 函数
    *            fn是真正完成更新偏移的函数
    */
   private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
                          (fn: SimpleConsumer => Any): Unit = {
     brokers.foreach { hp =>
       var consumer: SimpleConsumer = null
       try {
         consumer = connect(hp._1, hp._2)//每一次获取对应的consumer
         fn(consumer)
       } catch {
         case NonFatal(e) =>
           errs.append(e)
       } finally {
         if (consumer != null) {
           consumer.close()
         }
       }
     }
   }

  /**
    *
    * @param host 主机地址
    * @param port 主机端口号
    * @return  返回一个Consumer
    */
   def connect(host: String, port: Int): SimpleConsumer =
     new SimpleConsumer(host, port, config.socketTimeoutMs,
       config.socketReceiveBufferBytes, config.clientId)
 }
