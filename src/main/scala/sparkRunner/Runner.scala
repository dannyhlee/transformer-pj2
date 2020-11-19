package sparkRunner

import java.io.{BufferedWriter, File, FileWriter, IOException}

import scala.collection.mutable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks.{break, breakable}

object Runner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Runner")
      .master("local[*]")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("TRACE")

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

    val df = spark.read.schema(trendSchema).json("input")

    val file = new File("output.csv")

    if (!file.exists) file.createNewFile

    val file_writer = new FileWriter(file, true)

    val buffered_writer = new BufferedWriter(file_writer)

//    buffered_writer
//      .write("\"Trend Name\",\"Location\",\"Date\",\"Time\",\"Rank\",\"Tweet Volume\"\n")

    buffered_writer.close()

    def writeFile(lines: List[(Any, Any, Any, Long, Long)]) {
      try {
        val file = new File("output.csv")

        if (!file.exists) file.createNewFile

        val file_writer = new FileWriter(file, true)

        val buffered_writer = new BufferedWriter(file_writer)

        for (line <- lines) {
          println(line._3.toString, line._3.toString.substring(0, 10), line._3.toString.substring(11, 13) )
          breakable {
            if (line.productArity != 5) break
            else if (line._3.toString.length != 20) break
            else {
              println("writing...")
              buffered_writer
                // trend_name, location, date, hour, rank, tweet_volume
                .write("\"%s\",\"%s\",\"%s\",\"%s\",%d,%d\n"
                  .format(line._1, line._2, line._3.toString.substring(0, 10),
                    line._3.toString.substring(11, 13), line._4, line._5))
            }
          }
        }
        buffered_writer.close()
      } catch {
        case e: IOException => println(s"IOException: ${e.getMessage}")
      }
    }

    println("---------------------"+df.count())
    val colData = df.select("*").cache()

    colData.foreach(row => {
      val as_of = row.getString(0)

      val location = row
        .getAs[mutable.WrappedArray[Row]](2)
        .map(struct => struct(0))
        .mkString

      val trends = row
        .getAs[mutable.WrappedArray[Row]](3)
        .zipWithIndex.map { case (struct, index) =>
        // trend text, location, time stamp, rank, tweet volume
        (
          { if (null == struct(0)) "" else struct(0).toString },
          { if (null == location) "" else location },
          { if (null == as_of) ""  else as_of },
          (index + 1).toLong,
          { if (null == struct(3)) 0.toLong
            else if(!struct(3).toString.forall(_.isDigit)) 0.toLong
            else struct(3).toString.toLong }
        )
      }.toList

      writeFile(trends)
    })
  }
}


