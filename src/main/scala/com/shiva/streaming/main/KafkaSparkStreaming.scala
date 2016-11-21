package com.shiva.streaming.main


import com.shiva.streaming.factory.{ KafkaProducerFactory => KFK }

import com.shiva.streaming.factory.{ HBaseConnectionFactory => HCF }
import com.shiva.streaming.factory.{ LoggerFactory => ALogger }

import java.util.Properties
import scala.io.Source
import com.typesafe.config._
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcast }

import org.apache.hadoop.hbase.{ HConstants, HBaseConfiguration }
import org.apache.hadoop.hbase.client.{ Connection }
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Get, Put, Result }
import scala.xml._
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.InputDStream

class KafkaSparkStreaming{
  protected def printUsage {
    println("USAGE : ViewAcraReport <ConfigFilePath>")
  }

  protected def _Exit {
    System.exit(1)
  }
  
  def displayServiceName{ 
     println("-----------------------------")
     println("--- Kafka Spark Streaming ---")
     println("-----------------------------")
  }
  
  protected def getUserConfig(_configfilepath : String): Config = {
     val _configfile = _configfilepath+"/"+"config.properties"
     val config = ConfigFactory.parseFile(new File(_configfile))
     ConfigFactory.load(config)  
  }
  
  protected def getConfigValue(_config: Config, _key: String): Option[String] = try{
    _key match {           
      case _key: String if _key.nonEmpty => Some(_config.getValue(_key).render().replaceAll("\"", ""))
      case _key: String if _key.isEmpty() => None
    }    
  }catch{
    case ex: Exception => println("ERROR: Missing Config Property : ",_key); 
                          throw new RuntimeException("KeyError")
  }
  
  protected def getKafkaParams(_config: Config): Map[String,String] = {
     Map("metadata.broker.list" -> getConfigValue(_config, 
                                     "kafka_brokers").getOrElse("localhost:9092"),
         "group.id" -> getConfigValue(_config, "groupid").getOrElse("kafkaSparkStreaming")
    ) 
  }
  
  protected def getResProducerConfig(_config: Config): Properties = {
     val p = new Properties()
     p.setProperty("bootstrap.servers", getConfigValue(_config, "kafka_brokers")
                                       .getOrElse("localhost:9092")) 
     p.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
     p.setProperty("value.serializer", classOf[StringSerializer].getName)
     p     
  }
  
  protected def getHBaseConnParams(_config: Config): Properties = {
     val p = new Properties()
     p.setProperty(HConstants.ZOOKEEPER_QUORUM, getConfigValue(_config, "hbasezookeeper")
                   .getOrElse("localhost:2181"))
     p.setProperty("hbase.client.retries.number", Integer.toString(1));
     p.setProperty("zookeeper.session.timeout", Integer.toString(100));
     p.setProperty("zookeeper.recovery.retry", Integer.toString(1));
     p             
  }
}

object KafkaSparkStreaming {
  val obj = new KafkaSparkStreaming
  def main(args: Array[String]): Unit = {    
     if (args.length < 1) {
      obj.printUsage; obj._Exit
    }    
    run(args)
  }
  
 def run(args: Array[String]): Unit = {
    obj.displayServiceName
    println("Config File Path : ",args(0))
    val _confighndl = obj.getUserConfig(args(0))
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val streamingbatchinterval = obj.getConfigValue(_confighndl,
                                 "StreamingBatchInterval").getOrElse(2).toString()
      
    val _sparkconf = new SparkConf().setAppName("KafkaSparkStreaming")
    if (args.length > 1) {
      _sparkconf.setMaster(args(1))
    }else{
      _sparkconf.setMaster("local[*]")
    }
    
    val _sparkContext = new SparkContext(_sparkconf)
    val _ssc = new StreamingContext(_sparkContext, 
                                    Seconds(streamingbatchinterval.toLong))
    try {
      val _getreqTopic = obj.getConfigValue(_confighndl, 
                                            "requestTopic").getOrElse("reqTopic")
      val _reqtopic = Set(Some(_getreqTopic).mkString)     
       val kafkaProducer: Broadcast[KFK[Array[Byte], String]] = {
        _ssc.sparkContext.broadcast(KFK[Array[Byte], String]
        (obj.getResProducerConfig(_confighndl)))
      }
      val hbtable: Broadcast[HCF] = {
        _ssc.sparkContext.broadcast(HCF(obj.getHBaseConnParams(_confighndl)))
      }
    val responseTopic: Broadcast[String] = {
      _ssc.sparkContext.broadcast(obj.getConfigValue(_confighndl, 
                                            "responseTopic").getOrElse("resTopic"))
    }
   
    val _logger: Broadcast[ALogger] = {
      _ssc.sparkContext.broadcast(ALogger(this.getClass.getName))
    }
    val dstream = KafkaUtils.createDirectStream[String, 
                                                String, 
                                                StringDecoder,
                                                StringDecoder](_ssc,
                                                               obj.getKafkaParams(_confighndl),
                                                               _reqtopic)      
      dstream.map(_._2).foreachRDD {
        rdd => if (!rdd.isEmpty()) {
                  rdd.foreachPartition { 
                    req =>  
                      if (req.nonEmpty) {
                       try{ 
                          processStreams(req,
                                      kafkaProducer.value,
                                      hbtable.value,
                                      responseTopic.value,
                                      _logger.value)
                       }catch{
                         case ex: Exception => println(s"Exception Caught inside Executor : ${ex}")
                                              // log.info(s"Exception Caught inside Executor : ${ex}")
                       }finally{
                         println("Excecutor Task Completed...")
                       }
                      }
                    } /* foreach partition  match statement ends here */
               } /* idd is not empty case ends here */
      } /*foreachRDD Ends here */
      _ssc.start()
      _ssc.awaitTermination()

    } catch {
      case ix: InterruptedException => println("Interrupted...")
      case ex: Exception            => println("Exception Caught in Driver" + ex.printStackTrace())
    } finally {
      println("Closing Connections...")
      _ssc.stop(true, true)
    } /*main try catch ends here*/
  } /* Process Method Ends */
  
 def processStreams(_request: Iterator[String], 
                     kafkaProdcuer: KFK[Array[Byte], String],
                     hbtable: HCF,
                     responseTopic: String,
                     log: ALogger): Unit = {
     
    
    try {
      _request.foreach { _reqmsg =>
      if (_reqmsg.nonEmpty) {
        reqMessageProcessor(_reqmsg,
            kafkaProdcuer,
            hbtable,
            responseTopic,
            log)
        } else {
          println("Empty Request Ignored...")
          log.info("Empty Request Ignored...")
        }
      }

    } catch {
      case ex: Exception => throw ex
    } 
  
  }  
 
  def reqMessageProcessor(_reqMsg: String,
                          kafkaProducer: KFK[Array[Byte], String],
                          hbtable: HCF,
                          responseTopic: String,
                          log:ALogger) = {  
    
   val result = getRecord(hbtable.getConnection(), _reqMsg.trim())
  
   val empDetails = result.getValue(Bytes.toBytes("empdetails"),
            Bytes.toBytes("data"))
            
    try{
      println("Processed Successfully : ",responseTopic)
      kafkaProducer.send(responseTopic,result.getRow, Bytes.toString(empDetails))
    }catch{
      case ex: Exception => {
        println(s"Exception While Sending Response Message ${ex}")
        throw ex
      }                          
    }
  
  }
  
  def getRecord(hconn: Connection, empId: String): Result = {   
    val __htable = hconn.getTable(TableName.valueOf("emp"))  
    try {                       
         val get = new Get(Bytes.toBytes(empId))
         val res = __htable.get(get)
         res                        
         } catch {
             case ex: Exception => println("Exception Caught : ",ex); 
                                   throw ex 
         }finally {         
             __htable.close()
         }  
  }  
}