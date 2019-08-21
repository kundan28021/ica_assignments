package com.ica

import java.io.FileInputStream
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import java.io.FileNotFoundException
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.hadoop.fs.FileSystem
import java.io.File
import java.util.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import Constants._

object Utilities {

  def setLoggerProps() {
    /*to supress org and akka logs*/
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def getSparkContext(appName: String): SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sparkContext = new SparkContext(sparkConf)
    sparkContext
  }

  /*Read the properties file from HDFS*/
  def readPropertiesFromHdfs(hdfsFilePath: String): Properties = {

    val prop = new Properties()
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)

    try {
      val is = fs.open(new Path(hdfsFilePath))
      prop.load(is)
      is.close()
    } catch {
      case e: FileNotFoundException =>
        {
          log(s"$FILE_NOT_FOUND : $hdfsFilePath")
        }

    }
    prop
  }

  /*This method is to create Schmea for converting RDD into DataFrame*/
  def getSchemaFromString(schemaString: String, nameDelim: String, typeDelim: String): StructType = {
    val fields = schemaString.split(nameDelim).map(fieldName => StructField(fieldName.split(typeDelim)(0), getSchemaTypes(fieldName.split(typeDelim)(1)), nullable = true))
    val schema = StructType(fields)
    schema
  }

  def getSchemaTypes(dataType: String): String =
    dataType.toLowerCase() match {
      case int    => IntegerType
      case double => DoubleType
      case string => StringType
      case _      => "other"
    }

  def convertRDDTodf(sqlContext: SQLContext, inputRDD: RDD[Row], schema: StructType): DataFrame = {
    val dataframe = sqlContext.createDataFrame(inputRDD, schema)
    dataframe
  }

  def conStringRDDToRowRDD(inputRDD: RDD[String], delim: String): RDD[Row] = {
    val inputRowRDD = inputRDD.map(x => Row.fromSeq(x.split(delim)))
    inputRowRDD
  }

  /*This method is to scrubb the delimiter between records and replace missing values with  null*/
  def parsedLine(line: String, delim: String): String = {
    log(s"Record Line is : $line")
    line.replaceAll("\\s+", delim).replace("NaN", null)
  }

  def log(msg: String) = {
    val logMsg = new Date() + " : " + msg
    println(logMsg)

  }

  /*This method will read the configuration file which contains file name with schema details*/
  def readConfigurationFile(fileName: String): Option[Iterator[String]] = {

    try {
      val configDetais = Source.fromFile(fileName).getLines()
      Some(configDetais)
    } catch {
      case e: FileNotFoundException =>
        log(s"$FILE_NOT_FOUND : $fileName")
        None
    }

  }

  def processConfigFileLines(line: String) = {
    val configArray = line.split("|")
    val fileName = configArray(0)
    val schemaString = configArray(1)
    log(s"File Name is  : $fileName Schema String is : $schemaString")
    (fileName, schemaString)
  }
  /*This method is to laod hdfs file to Hive tables*/
  def loadHdfsToHiveTable(sparkContext: SparkContext, hdfsDir: String, hdfsFileName: String, schemaString: String, finalTable: String) = {
    val sqlContext = new SQLContext(sparkContext)
    val hiveContext = new HiveContext(sparkContext)
    val hdfsFilePath = hdfsDir + "/" + hdfsFileName
    val inputRDD = sparkContext.textFile(hdfsFilePath)
    Utilities.log(s"Count of record for file: $hdfsFileName " + inputRDD.count())

    /*Sovling uneven space between field here Assumping there is no space between fileds value*/
    val parsedInputRDD = inputRDD.map(line => Utilities.parsedLine(line, RECORD_FIELD_DELIM))

    val inputRowRDD = Utilities.conStringRDDToRowRDD(parsedInputRDD, RECORD_FIELD_DELIM)
    val schema = Utilities.getSchemaFromString(schemaString, SCHEMA_NAME_DELIM, SCHEMA_TYPE_DELIM)
    val inputTempDf = Utilities.convertRDDTodf(sqlContext, inputRowRDD, schema)
    inputTempDf.registerTempTable("INPUT_TEMP_DATA")
    hiveContext.sql(s"INSERT INTO $finalTable select * from INPUT_TEMP_DATA")
    Utilities.log(s"Data is inserted to table $finalTable")

  }
  /*This method is to create all the necessary hive tables required.*/
  def createHiveTable(sc: SparkContext, query: String) = {
    Utilities.log(s"Create Query is $query")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql(query)
    Utilities.log(s"Query has been executed successfully")
  }

  /*This  method is to test the records in the filea and respective Hive table*/
  def testRecordCount(hdfsPath:String, sc: SparkContext, query: String) = {
    val allRecordRDD=sc.textFile(hdfsPath)
    val recordCout = allRecordRDD.count()
    val hiveContex = new HiveContext(sc)
    val tableRecordCount = hiveContex.sql(query)
    if (tableRecordCount == tableRecordCount) {
      Utilities.log(s"Records are mathing in the  table $tableRecordCount")
    } else {
      Utilities.log(s"Records are not mathing in the  table $tableRecordCount")
    }

  }

}