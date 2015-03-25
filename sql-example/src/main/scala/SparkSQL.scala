import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkSQL {
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)

  def main(args: Array[String]) = {
  	val conf = new SparkConf().setAppName("SQL example")
  	val sc = new SparkContext(conf)

	// sc is an existing SparkContext.
	val sqlContext = new SQLContext(sc)

	// this is used to implicitly convert an RDD to a DataFrame.
	import sqlContext.implicits._

	// Create an RDD of Person objects and register it as a table.
	val people = sc.textFile(sys.env.get("SPARK_HOME").get  + "/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

	people.registerTempTable("people")

	// SQL statements can be run by using the sql methods provided by sqlContext.
	val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

	// The results of SQL queries are SchemaRDDs and support all the
	// normal RDD operations.
	// The columns of a row in the result can be accessed by ordinal.
	teenagers.map(t => "Name: " + t(0)).collect.foreach(println)
  }
}
