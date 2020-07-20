package qq

//import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.api.r.SQLUtils

object mix_spark_scala {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local").appName("sachin tlidhu").getOrCreate()
    val df = ss.read.option("header","true").option("inferSchema","true").csv("F:/files/")
    df.show()
    df.where("sno = 1").show()
    import ss.implicits._
   /* df.createOrReplaceGlobalTempView("temp")
    val a = ss.sql("select sno from temp")
    a.show()*/
    var num = 0
    while(num<=5){
      //df.select("sno").show(1)
      //df.count()
      println(""+num)
      num +=1
    }

    //if(df.select("sno").show(1) ){
      //println("wwow")
    //}

  }

}
