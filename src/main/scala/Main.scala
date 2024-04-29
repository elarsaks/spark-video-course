package sparkvideocourse

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark-video-course")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("data/AAPL.csv")

    df.show()
  }
}