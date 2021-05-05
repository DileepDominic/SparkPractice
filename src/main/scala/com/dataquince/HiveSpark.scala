package com.dataquince

import org.apache.spark.sql.SparkSession

object HiveSpark {
  def main(args: Array[String]): Unit = {

    //For windoww
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val sc = SparkSession
      .builder
      .master("local[*]")
      .appName("Hive Spark main program")
      .enableHiveSupport()
      .getOrCreate()

    val sequence = Seq((1, "A"), (2, "B"), (3, "C"))

    val df = sc.createDataFrame(sequence).toDF("col1", "col2")
    df.show()

    //Write to hive table
    df.write.format("csv").save("C:\\hadoop\\dataframe.csv")
  }
}
