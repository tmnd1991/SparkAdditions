name := "SparkAdditions"

version := "1.0-spark_1.6.1"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint")

scalacOptions ++= Seq("-target:jvm-1.7")

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"