package com.ica
import Constants._

object Master extends App {

  //Config file should be place at the  same location of  the jar files.
  val fileType = args(0)

  /*There must be one argument passed to this job.*/
  if (args.length != 1) {
    Utilities.log("pease pass one argument  to the job")
    System.exit(1)
  }
  Utilities.setLoggerProps
  val sparkContext = Utilities.getSparkContext(APP_NAME)

  if (fileType.toUpperCase() == "T") {
    Utilities.createHiveTable(sparkContext, ICA_TEMPERATURE_HIVE_TBL)
    val fileConfig = Utilities.readConfigurationFile("temp_config.txt")
    if (!fileConfig.get.isEmpty) {
      while (fileConfig.get.hasNext) {
        val configLine = fileConfig.get.next().asInstanceOf[String]
        val outputLine = Utilities.processConfigFileLines(configLine)
        val fileName = outputLine._1
        val schemaString = outputLine._2
        Utilities.log(s"File To Process :$fileName Schema String :$schemaString")
        Temperature.loadAllTeamperatureData(sparkContext, fileName, schemaString,ICA_TEMPERATURE_HIVE_TBL)
      }

    }
    Utilities.testRecordCount(ICA_TEMPRATURE_HDFS_DIR,sparkContext,TMPR_HIVE_TABLE_COUNT)

  } else if (fileType.toUpperCase() == "p") {
    Utilities.createHiveTable(sparkContext, ICA_PRESSURE_HIVE_TBL)
    val fileConfig = Utilities.readConfigurationFile("pressure_config.txt")

    if (!fileConfig.get.isEmpty) {
      var index: Int = -1
      while (fileConfig.get.hasNext) {
        index += 1
        val configLine = fileConfig.get.next().asInstanceOf[String]
        val outputLine = Utilities.processConfigFileLines(configLine)
        val fileName = outputLine._1
        val schemaString = outputLine._2
        Utilities.log(s"File To Process :$fileName Schema String :$schemaString")
        if (index == 0) {
          Utilities.createHiveTable(sparkContext, PSRE_HIVE_TBL_CREATE_1756_1858)
          Pressure.loadPressureData(sparkContext, fileName, schemaString,PSRE_HIVE_TBL_CREATE_1756_1858)
        }
        if (index == 1) {
          Utilities.createHiveTable(sparkContext, PSRE_HIVE_TBL_CREATE_1859_1861)
          Pressure.loadPressureData(sparkContext, fileName, schemaString,PSRE_HIVE_TBL_CREATE_1859_1861)
        } else {
          Pressure.loadAllPressureData(sparkContext, fileName, schemaString,ICA_PRESSURE_HIVE_TBL)
        }

      } //End of while Loop

    }

  } else {
    Utilities.log("Please Pass the Valid file Type eitehr T or P")
  }
}