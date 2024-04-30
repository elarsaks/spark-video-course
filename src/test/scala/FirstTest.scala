package sparkvideocourse

import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class FirstTest extends AnyFunSuite with Matchers {

  private val spark = SparkSession
    .builder()
    .appName("Returns Highest closing price per year")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(
    Seq(
      StructField("date", DateType, nullable = true),
      StructField("value1", DoubleType, nullable = true),
      StructField("value2", DoubleType, nullable = true)
    )
  )

  test("Test with sample data"){
    val testRows = Seq(
      Row(Date.valueOf("2022-02-12"), 1.0, 2.0),
      Row(Date.valueOf("2022-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2022-01-12"), 1.0, 3.0)
    )
    val expected = Seq(
      Row(Date.valueOf("2022-02-12"), 1.0, 2.0),
      Row(Date.valueOf("2022-01-12"), 1.0, 3.0)
    )
    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testDf = spark.createDataset(testRows)
    val actualRows = Main.highestClosingPricesPerYear(testDf).collect()

    actualRows should contain theSameElementsAs  expected
  }
}
