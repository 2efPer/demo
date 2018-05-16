import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object StayStatistic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(args(0))
      .appName("Stay-Statistic")
      .getOrCreate()

    val rawdata = spark.read.load(args(1)).map(r=>{r.getString(0).split(",")}).toDF("user","location","starttime","duration")

    val frame1 = rawdata.groupBy("user","location").min("starttime").toDF("user","location","starttime")
    val frame2 = rawdata.groupBy("user","location").sum("duration").toDF("user","location","totaltime")
    val res = frame1.join(frame2,Seq("user","location")).map(r=>{
      (r.getString(0),r.getString(1),toDate(r.getLong(2)),r.getLong(3))
    }).toDF("user","location","starttime","totaltime").cache()
    res.show()
  }

  def toLongDate(str:String):Long={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.parse(str).getTime
  }
  def toDate(time:Long):String={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(time)
  }

}
