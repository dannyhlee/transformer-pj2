package sparkRunner

//import java.net.URI

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Runner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Runner")
      .master("local[*]")
      .getOrCreate()

    readFilesAsJson(spark)
  }

  def readFilesAsJson(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

//    val raw_df = spark.read.json("test.json")
//    raw_df.show(truncate=false)

    /**
     * '''raw_df.printSchema()''' - ''Spark generated schema''
     * <pre>
     *     root
     * |-- as_of: string (nullable = true)
     * |-- created_at: string (nullable = true)
     * |-- locations: array (nullable = true)
     * |    |-- element: struct (containsNull = true)
     * |    |    |-- name: string (nullable = true)
     * |    |    |-- woeid: long (nullable = true)
     * |-- trends: array (nullable = true)
     * |    |-- element: struct (containsNull = true)
     * |    |    |-- name: string (nullable = true)
     * |    |    |-- promoted_content: string (nullable = true)
     * |    |    |-- query: string (nullable = true)
     * |    |    |-- tweet_volume: long (nullable = true)
     * |    |    |-- url: string (nullable = true)
     * </pre>
     **/
    val trendSchema = new StructType()
      .add("as_of", StringType)
      .add("created_at", StringType)
      .add("locations", ArrayType(new StructType()
          .add("name", StringType)
          .add("woeid", LongType)
        ))
        .add("trends", ArrayType(new StructType()
          .add("name", StringType)
          .add("promoted_content", StringType)
          .add("query", StringType)
          .add("tweet_volume", LongType)
          .add("url", StringType)
        ))

    val df = spark.read.schema(trendSchema).json("test.json")
//    df.printSchema()
    df.show(false)

    df.createOrReplaceTempView("trends")
//    val locationsDF=spark.sql("select locations from trends")
//    locationsDF.show(false)

    val locationTrendsDF = df.select($"locations.name", explode($"trends"))
    locationTrendsDF.show(false)

    val trendsDF = df.select(explode($"trends"))
//      .select($"name")
    trendsDF.printSchema()
    trendsDF.show(false)

    val res = df.withColumn("trends", explode($"trends"))
      .select("*")

    res.show(false)

    val res2 = res.select(res.col("trends.name"))

    res2.show(false)

//    val res3 = res2.collect.map(_.toSeq)
//    this doesn't work, wrappedArray -^
//    val res3 = res2.collect.map(row => row.getString(0))
//      this works -^
    val res3 = res2.collect.flatMap(_.toSeq)
//      this works -^

    res3.foreach(println)
    println(res3.length)

  }
}


