import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkJoin {
  def main(args: Array[String]) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val conf = new SparkConf().setAppName("Join Spark")
    val sc = new SparkContext(conf)
 
    case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)
    case class Click (d: java.util.Date, uuid: String, landing_page: Int)
 
    val reg = sc.textFile("reg.tsv").map(_.split("\t")).map(
    r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
    )
 
    val clk = sc.textFile("clk.tsv").map(_.split("\t")).map(
    c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt))
    )

    val tuples = reg.join(clk).take(2)
    println(s"Result of join: ${tuples(0)} ${tuples(1)}")
  }
}
