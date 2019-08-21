package com.ica

import Constants._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext

object Temperature {

  def loadAllTeamperatureData(sparkContext: SparkContext, hdfsFileName: String, schemaString: String,finalTable:String) = {

    Utilities.loadHdfsToHiveTable(sparkContext, ICA_TEMPRATURE_HDFS_DIR, hdfsFileName, schemaString,finalTable )

  }

}