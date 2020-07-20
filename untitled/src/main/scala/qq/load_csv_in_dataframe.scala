package qq

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object load_csv_in_dataframe {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("load csv").master("local").getOrCreate()

    val loadcsv = ss.read
      //.option("header","true").option("inferSchema","true")
      //or use below, both r right
      .options(Map("header"->"true","inferSchema"->"true"))
      .csv("F:/files/")

    val schema = StructType(StructField("sno",LongType,true)::StructField("fname",StringType,true)
      ::StructField("lname",StringType,true)
      ::StructField("deoe",StringType,true)::Nil)

    val newdf = ss.read.option("header","true").schema(schema).csv("F:/files/")

    loadcsv.show()
    loadcsv.printSchema()
    newdf.printSchema()
    newdf.show()

    val newcol = newdf.columns
    newcol.mkString(" , ").foreach(print)
    newdf.describe("sno").show
    newdf.dtypes.foreach(println)

    println("dfwemgkg")
    newdf.select("sno","fname","deoe").groupBy("deoe").count().show()
    println("mdlmmdmdmdmdmd")
    //loadcsv.show()
    println("smdkwqjdiwqhdwqihwquifh")
    //newdf.show()
  }

}
