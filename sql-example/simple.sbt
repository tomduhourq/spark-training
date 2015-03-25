name := "sql-example"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.3.0",
			    "org.apache.spark" % "spark-sql_2.10" % "1.3.0"
)
