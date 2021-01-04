
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.commons.io.FilenameUtils
import java.io._
import spray.json._

import org.apache.spark.sql.functions.{col, udf}
import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.io.Path



object Launcher {

  def main(args: Array[String]): Unit = {

    val transformField = udf((date: String) => {
      val formatDate = new SimpleDateFormat("ddMMyyyy")
      formatDate.format(new Date(date))
    })

    //Add Scopt command line
    Logger.getLogger("org").setLevel(Level.OFF)

    //create sparksession object
    implicit val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val titles = new File("data/").listFiles.map(_.getName).toList
    titles.foreach{println}

    val schema = StructType(
      StructField("amount", IntegerType, true) ::
        StructField("base_currency", StringType, true) ::
        StructField("currency", StringType, true) ::
        StructField("exchange_rate", DoubleType, true) ::
        StructField("date", StringType, true) :: Nil)

    val dataRDD = sparkSession.sparkContext.emptyRDD[Row]
    var alldf = sparkSession.createDataFrame(dataRDD,schema)

    var temp = alldf

    for (file <- titles ) {
      val df: DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", false).csv("data/" + file.toString)
      val filenamewithoutext = FilenameUtils.removeExtension(file)

      val input_format = new SimpleDateFormat("ddMMyyyy")
      val output_format = new SimpleDateFormat("dd-MM-yyyy")

      val formatted_Date = output_format.format(input_format.parse(filenamewithoutext))
      var dfwithdate = df.withColumn("date", lit(formatted_Date))
      alldf = alldf.union(dfwithdate)
      //alldf.show()
      //println(alldf.schema)

      val date_count = alldf.filter(col(colName = "date") === formatted_Date)
      val nb = date_count.count()
      println(nb)

      if (nb < 8) {
        val df_limit = temp.limit(8 - nb.toInt)
        val new_df = dfwithdate.union(df_limit)
        new_df.show()
        new_df.groupBy("date").count().show()

      }

      temp = dfwithdate
    }

    alldf.write.partitionBy("date").mode(SaveMode.Overwrite).parquet("result")

    val json = sparkSession.read.json("conf/config.json")
    val jconf = json.collect()
    var day_ago = jconf(2)(0).toString
    day_ago = day_ago.replace("  \"fillWithDaysAgo\": ", "")
    val day_ago_int = day_ago.toInt
    println(day_ago_int)

    var file_day = jconf(1)(0).toString
    file_day = file_day.replace("  \"date\": ", "").replace("\"","").replace(",","")
    println(file_day)









  }
}

