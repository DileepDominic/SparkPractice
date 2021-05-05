package com.dataquince

import org.apache.spark.sql.{SparkSession,SaveMode}

object MainFile {
  def main(args: Array[String]): Unit = {

    val sc = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark main program")
      .getOrCreate()

    val sequence = Seq((1, "A"), (2, "B"), (3, "C"))

    val df = sc.createDataFrame(sequence).toDF("col1", "col2")
    df.show()

  }
}
