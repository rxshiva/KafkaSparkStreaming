package com.shiva.streaming.factory

 import org.apache.hadoop.hbase.client.HTable
 import org.apache.hadoop.hbase.client.Get
 import java.util.concurrent.Future
 import org.apache.hadoop.hbase.client.Result
 import org.apache.hadoop.hbase.client.{ConnectionFactory, Connection}
 import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration,TableName}
 import org.apache.hadoop.conf.Configuration
 import org.apache.hadoop.hbase.mapreduce.TableInputFormat
 import org.apache.hadoop.hbase.client._

class HBaseConnectionFactory(CreateHConnection: () => Connection) extends Serializable{
   
  @transient lazy val htable = CreateHConnection()  
  
  def getHTable(_htablename: String): Table = {
    htable.getTable(TableName.valueOf(_htablename))
  }
  
  def getConnection(): Connection = {
    this.htable
  }
  
}

object HBaseConnectionFactory{
   import scala.collection.JavaConversions._
   import scala.util.Properties
   import org.apache.hadoop.security.UserGroupInformation
   
  def apply(_config: java.util.Properties) : HBaseConnectionFactory  ={
    
    val CreateHConnection = () => {
      
      val _hconf = HBaseConfiguration.create();
     
      val config = _config.toMap
       config.foreach( x => { _hconf.set(x._1,x._2.toString()) } )          
      val _connection = ConnectionFactory.createConnection(_hconf)
     
      sys.addShutdownHook {
        println("Closing HConnection...")
        _connection.close()
      }
      _connection
    }
    new HBaseConnectionFactory(CreateHConnection)
  }
  
}