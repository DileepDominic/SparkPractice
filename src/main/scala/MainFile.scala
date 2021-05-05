import org.apache.spark.sql.SparkSession

object MainFile {
  def main(args: Array[String]): Unit = {
      print("Test line")

    val sc = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.main", "local")
      .getOrCreate()

    println("")
    val sequence = Seq((1,"A"),(2,"B"),(3,"C"))

    val df = spark.createDataFrame(sequence).toDF("col1","col2")
    df.show()

  }
}
