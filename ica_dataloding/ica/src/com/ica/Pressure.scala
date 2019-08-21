package com.ica

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import Constants._

object Pressure {

  def loadAllPressureData(sparkContext: SparkContext, hdfsFileName: String, schemaString: String,finalTable:String) = {
    Utilities.loadHdfsToHiveTable(sparkContext, ICA_PRESSURE_HDFS_DIR, hdfsFileName, schemaString, finalTable)

  }
  
  def loadPressureData(sparkContext: SparkContext, hdfsFileName: String, schemaString: String,tableName:String) = {
    Utilities.loadHdfsToHiveTable(sparkContext, ICA_PRESSURE_HDFS_DIR, hdfsFileName, schemaString, tableName)

  }
}