package sparkRunner

import java.io.{BufferedWriter, File, FileNotFoundException, FileWriter, IOException, PrintWriter}

import scala.collection.mutable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

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
//      .write("\"Trend Text\",\"Location\",\"Timestamp\",\"Rank\",\"Tweet Volume\"\n")

    buffered_writer.close()

    def writeFile(lines: List[(Any, Any, Any, Int, Int)]) {
      try {
        val file = new File("output.csv")

        if (!file.exists) file.createNewFile

        val file_writer = new FileWriter(file, true)

        val buffered_writer = new BufferedWriter(file_writer)

        for (line <- lines) {
          buffered_writer
            .write("\"%s\",\"%s\",\"%s\",%d,%d\n"
              .format(
                { if (null == line._1) 0 else line._1.toString },
                { if (null == line._2) 0 else line._2 },
                { if (null == line._3) 0 else line._3 },
                { if (null == line._4) 0 else line._4 },
                { if (null == line._5) 0 else line._5.toString.toInt }
              ))
        }
        buffered_writer.close()
      } catch {
        case e: IOException => println("IOException: Could not write to file")
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
          { if (null == struct(0)) 0 else struct(0) },
          { if (null == location) 0 else location },
          { if (null == as_of) 0 else as_of },
          { if (null == index + 1) 0 else index + 1 },
          { if (null == struct(3)) 0 else struct(3).toString.toInt }
        )
      }.toList

      writeFile(trends)

    })



  }
}


