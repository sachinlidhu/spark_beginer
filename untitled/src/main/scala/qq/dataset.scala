package qq

import org.apache.spark.sql.SparkSession
case class rating(sno : Integer,fname :String,lname: String,dept : String)
object dataset extends App {
  //def main(args: Array[String]): Unit = {
  val ss = SparkSession.builder().appName("dataset").master("local").getOrCreate()
 // }
  import ss.implicits._
  val dset = ss.read.option("header","true").option("inferSchema","true").csv("F:/files/").as[rating]
  dset.show()

}
/*object dataset{
  def main(args: Array[String]): Unit = {
    println("nckjfwf")
    val ss = SparkSession.builder().appName("dsysfet").master("local").getOrCreate()
    import ss.implicits._
    val dset = ss.read.option("header","true").option("inderSchema","true").csv("F:/files/").as[rating]
    dset.show()
  }
}
*/