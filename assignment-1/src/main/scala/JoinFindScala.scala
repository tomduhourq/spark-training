import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object JoinFindScala {

  def wordCountTransform(rdd: RDD[String]) = {
    rdd.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Join and find Scala")
    val sc = new SparkContext(conf)

    val readme = sc.textFile(
	sys
	.env
	.get("SPARK_HOME")
	.getOrElse("SPARK_HOME not found") + "/README.md"
    )
    val changes = sc.textFile(
	sys
	.env
	.get("SPARK_HOME")
	.getOrElse("SPARK_HOME not found") + "/CHANGES.txt"
    )
    
    val wcReadme  = wordCountTransform(readme)
    val wcChanges = wordCountTransform(changes)

    val joinedRDDs = wcReadme.join(wcChanges)
    
    // This could've been done in the wc pipeline
    val filterScala = joinedRDDs.filter(_._1.contains("Scala")).collect
    val filterSpark = joinedRDDs.filter(_._1.contains("Spark")).collect

    println(s"Joined with Scala: ${filterScala(0)}\nJoined with Spark: ${filterSpark(0)}")
  }
}
