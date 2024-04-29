package sparkvideocourse

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark-video-course")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    df.select("Date", "Open", "Close").show
    val column = df("Date")
    col("Date")
    import spark.implicits._
    $"date"

    // df.select(col("Date"), $"Open", df("Close")).show()
    df.select(column, $"Open", df("Close")).show()
  }
}