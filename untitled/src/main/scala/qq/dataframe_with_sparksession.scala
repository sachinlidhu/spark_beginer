package qq

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object dataframe_with_sparksession {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("dataframe with sparksession").master("local").getOrCreate()

    val rdd = ss.sparkContext.parallelize(Array(1,2,3,4,5))

    val schema = StructType(StructField("integre as strning", IntegerType, true)::Nil)

    val newrdd = rdd.map(element => Row(element))

    val df = ss.createDataFrame(newrdd,schema)
    df.show()
    df.printSchema()
  }


}
