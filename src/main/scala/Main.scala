package sparkvideocourse

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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

    val column = df("Open")
    val newColumn = column + (2.0) // column.plus(2.0)
    val columnString =  column.cast(StringType)

    df.select(column, newColumn, columnString)
      .filter(newColumn > 2.0)
      .filter(newColumn > column).as("OpenIncreasedBy2")
      .filter(newColumn === column).as("OpenAsString")
      .show()

    val litCol = lit(2.0)
    val newColumnString = functions.concat(columnString, lit("Hello World"))

    df.select(column, newColumn, columnString, newColumnString)
      .show()


    /*
    df.select("Date", "Open", "Close").show
    val column = df("Date")
    col("Date")
    import spark.implicits._
    $"date"

    // df.select(col("Date"), $"Open", df("Close")).show()
    df.select(column, $"Open", df("Close")).show()
    */
  }
}