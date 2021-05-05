package com.dataquince

import org.apache.spark.sql.SparkSession
import java.util.Properties

import java.util.Properties

object DBtoSpark {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession
      .builder
      .master("local[*]")
      .appName("Postgres Database to Spark program")
      .enableHiveSupport()
      .getOrCreate()

    val dbConnect = new Properties()
    dbConnect.put("user","postgres")
    dbConnect.put("password","xxxxxx")

    val dbTable = "dataquince.blog_category"
    val df = sc.read.jdbc("jdbc:postgres://localhost:5432/postgres",dbTable,dbConnect)
    df.show()
  }
}
