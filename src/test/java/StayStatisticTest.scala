import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object StayStatisticTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Stay-Statistic")
      .getOrCreate()
    val rawdata = spark.createDataFrame(Seq(
      ("user1","location1","2018-04-10 00:42:23",60),
      ("user1","location1","2018-04-12 10:42:23",5),
      ("user1","location1","2018-04-15 09:42:23",30),
      ("user2","location2","2018-01-11 01:42:23",30),
      ("user2","location2","2018-03-17 17:42:23",90),
      ("user3","location1","2018-03-14 14:42:23",20),
      ("user3","location1","2018-04-20 12:42:23",10),
      ("user4","location2","2018-01-16 23:42:23",80),
    )).map(r=>(r.getString(0),r.getString(1),toLongDate(r.getString(2)),r.getInt(3))).
      toDF("user","location","starttime","duration").repartition(3)
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
