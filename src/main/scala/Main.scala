package sparkvideocourse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, row_number, year}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.types.StringType
// import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

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

    val remameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume"),
    )


    val stockData = df.select(remameColumns: _*)

    import spark.implicits._

    stockData
      .groupBy(year($"date").as("year"))
      .agg(functions.max($"close").as("maxClose"), functions.avg($"close").as("avgClose"))
      .sort($"maxClose".desc)
      .show()

    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    stockData
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
      .explain(extended = true)


    /*
    stockData
      .groupBy(year($"date").as("year"))
      .max("close", "high")
      .show()

    val stockData = df.select(remameColumns: _*)
      .withColumn("diff", col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1)

    stockData.show()


    // df.select(df.columns.map(c => col(c).as(c.toLowerCase())): _*).show()

    val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampExpression")
    val timestampFromFunctions = current_timestamp().cast(StringType)

    df.select(timestampFromExpression, timestampFromFunctions).show()
    df.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp").show()

    df.createTempView("df")
    spark.sql("select * from df").show()

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