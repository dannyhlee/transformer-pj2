package sparkRunner

import java.net.URI

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

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
     * '''raw_df.show()''' - ''Spark generated schema''
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

    val singleTrendSchema = new StructType()
      .add("trends", ArrayType(new StructType()
        .add("name", StringType)
        .add("promoted_content", StringType)
        .add("query", StringType)
        .add("tweet_volume", LongType)
        .add("url", StringType)
      ))

    val df = spark.read.schema(trendSchema).json("input-old")
    df.printSchema()
    df.show(false)

    df.createOrReplaceTempView("trends")
    val locationsDF=spark.sql("select locations from trends")
    locationsDF.show(false)

    val trendsDF=spark.sql("select trends from trends")
    trendsDF.show(false)

    val fs = FileSystem.get(new URI("hdfs://localhost:9000/"), spark.sparkContext.hadoopConfiguration)
    println(spark.sparkContext.hadoopConfiguration)
    val fsStatus = fs.listStatus(new Path("hdfs://localhost:9000/"))
    fsStatus.foreach(x=> println(x.getPath))

    val outputPath = new Path("/user/spark/trends")
    println(fs.exists(outputPath), outputPath)
    if (fs.exists(outputPath))
      fs.delete(outputPath, true)

    println("Writing...")
    trendsDF.rdd.saveAsTextFile("hdfs://localhost:9000/user/spark/trends")
    println("Finished writing.")

    println("Reading...")
    val dfFromFile = spark.read.text("hdfs://localhost:9000/user/spark/trends")
    println("Finished Reading.")

    println("dfFromFile output")
    dfFromFile.show(false)

  }
}


