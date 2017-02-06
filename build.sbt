name := "word-count"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  // groupid    artifactid    version
  "org.apache.spark" %% "spark-core" % "2.0.2" % "compile",
  "org.apache.spark" %% "spark-sql" % "2.0.2" % "compile",
  "com.typesafe" % "config" % "1.3.1" % "compile"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "3.8.8" % "compile",
  "org.specs2" %% "specs2-junit" % "3.8.8" % "compile"
)

scalacOptions in Test ++= Seq("-Yrangepos")
