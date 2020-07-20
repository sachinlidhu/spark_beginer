package qq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object q1 {

  def main(args: Array[String]): Unit = {

    val sparksession = SparkSession.builder().appName("sparkfirst").master("local").getOrCreate()

    val array =Array(1,2,3,4,5)
    val arrayrdd = sparksession.sparkContext.parallelize(array,2)

    arrayrdd.foreach(println)

    println(s"kkkkksachinnnnn")
    val file = "F:/files/"
    println("sjkck")
    val filerdd = sparksession.sparkContext.textFile(file)
    println("www")
    println("no of records ::" + filerdd.count())
    //filerdd.foreach(println)
    println("hhhh")
    filerdd.take(5).foreach(println)
    println(s"sachinnnnn")
    filerdd.take(4).foreach(println)
    val e=filerdd
    e.foreach(println)

    val header = e.first()
    val csvwithoutheader = e.filter(_ !=header)
    csvwithoutheader.foreach(println)
    val f = csvwithoutheader.map(_(1))
    val g = csvwithoutheader.map(_.split(",")(1))
    f.foreach(println)
    g.foreach(println)
    header.foreach(println)
    header.take(1).foreach(println)
    //e.first().foreach(println)
    val header1 = e.filter(_ == header)
    //header1.foreach(println)
    //val headerrdd = sparksession.sparkContext.parallelize(header1, 1).
    header1.saveAsTextFile("F:/header1")
    //header.
  }

}
