package qq

import org.apache.spark.sql.SparkSession

object performance_diff extends App{

  val ss = SparkSession.builder().appName("ferformance").master("local").getOrCreate()

  //var start_time : Long = 0 //must be var(time), otherwise 0 will get permanently stuck to date
  //var end_time: Long = 0
  val start_time  = System.currentTimeMillis() //here u can use val,

  val read = ss.read.option("header","true").option("inferSchema","true").csv("F:/files/")

  val readfile = read.filter("sno>1").show()
  val end_time= System.currentTimeMillis()
   println("time to calculate dataframe is "+ (end_time-start_time)/1000)

  case class dset(sno:Integer,fname:String,lname:String,dept:String)
  val start_time_dst = System.currentTimeMillis()
  import ss.implicits._
  val readdst = ss.read.option("header","true").option("inferSchema","true").csv("F:/files/").as[dset]
  readdst.filter("sno>1").show()
  val end_time_dst = System.currentTimeMillis()
  println("dataset time os::" + (end_time_dst-start_time_dst)/1000.0)

}
