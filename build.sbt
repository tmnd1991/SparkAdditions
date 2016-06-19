name := "SparkAdditions"

version := "1.0-spark_1.6.1"

scalaVersion := "2.10.6"

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
