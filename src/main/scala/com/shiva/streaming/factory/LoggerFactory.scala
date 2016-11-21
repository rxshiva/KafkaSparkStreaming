package com.shiva.streaming.factory

import org.apache.log4j.Logger

class LoggerFactory(create_logger: () => Logger) extends Serializable {
 
  @transient lazy val log =  create_logger()
 
  def info(msg: String) = {
    log.info(msg)
  }
  
  def error(msg: String) = {
    log.error(msg)
  }
  
  def warn(msg: String) = {
    log.warn(msg)
  }
  
  def getLogger(): Logger ={
    this.log
  }
}

object LoggerFactory{
 def apply(loggerClass: String) : LoggerFactory  = {
    
    val CreateLogger = () => {
      
      val _log = org.apache.log4j.LogManager.getLogger(loggerClass)    
      sys.addShutdownHook {
        println("Closing Logger...")
        org.apache.log4j.LogManager.shutdown()
      }
      _log
    }
    new LoggerFactory(CreateLogger)
  }
 

}