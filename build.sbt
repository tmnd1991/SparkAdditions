name := "SparkAdditions"

version := "0.0.2-SNAPSHOT"

scalaVersion := "2.10.6"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint")

scalacOptions ++= Seq("-target:jvm-1.7")

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "7.0.13"