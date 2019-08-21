package com.ica

import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.FileNotFoundException

object Constants {

  
  val PROP_FILE_PATH = "/ica/properties/ica.properties"
  val prop = Utilities.readPropertiesFromHdfs(PROP_FILE_PATH)
  
  val APP_NAME = "ICA_ANALYSIS"
  
  //Hdfs File Path
  val ICA_TEMPRATURE_HDFS_DIR = prop.getProperty("ICA_TEMPRATURE_HDFS_DIR")
  val ICA_PRESSURE_HDFS_DIR = prop.getProperty("ICA_PRESSURE_HDFS_DIR")
  
  //Record and Schema Specific Details
  val RECORD_FIELD_DELIM=prop.getProperty("RECORD_FIELD_DELIM")
  val SCHEMA_NAME_DELIM=prop.getProperty("SCHEMA_NAME_DELIM")
  val SCHEMA_TYPE_DELIM=prop.getProperty("SCHEMA_TYPE_DELIM")
  
  //Tables Realted to Pressures
  val ICA_TEMPERATURE_HIVE_TBL=prop.getProperty("TMPR_HIVE_TABLE")
  val ICA_PRESSURE_HIVE_TBL=prop.getProperty("PSRE_HIVE_TABLE")
  val TMPR_HIVE_TABLE_CREATE=prop.getProperty("TMPR_HIVE_TABLE_CREATE")
  val TMPR_HIVE_TABLE_CREATE_PRT=prop.getProperty("TMPR_HIVE_TABLE_CREATE_PRT")
  
  //Tables realted to Pressures
  val PSRE_HIVE_TBL_CREATE=prop.getProperty("PSRE_HIVE_TBL_CREATE")
  val PSRE_HIVE_TBL_CREATE_PRT=prop.getProperty("PSRE_HIVE_TBL_CREATE_PRT")
  val PSRE_HIVE_TBL_CREATE_1756_1858=prop.getProperty("PSRE_HIVE_TBL_CREATE_1756_1858")
  val PSRE_HIVE_TBL_CREATE_1859_1861=prop.getProperty("PSRE_HIVE_TBL_CREATE_1859_1861")
  
  val PARTITION_COL=prop.getProperty("PARTITION_COL")
  val TMPR_HIVE_TABLE_COUNT=prop.getProperty("TMPR_HIVE_TABLE_COUNT")
  val FILE_NOT_FOUND="File is  not Found at the required Path"
  
 

}