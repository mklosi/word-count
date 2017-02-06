assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", tail@_*) => MergeStrategy.last
  case PathList("javax", "inject", tail@_*) => MergeStrategy.last
  case PathList(f) if f == "config.xml" || f == "mime.types" || f == "ss-config.xml" =>
    MergeStrategy.discard
  case PathList("com", "google", "common", tail@_*) => // required for spark-streaming-kinesis-asl
    MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", tail@_*) => // required for spark-streaming-kinesis-asl
    MergeStrategy.first
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) | ("mailcap" :: Nil) | ("license.txt" :: Nil) | ("notice.txt" :: Nil) | ("notice" :: Nil) | ("license" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case  "maven" :: tail if tail.lastOption.exists(_.startsWith("pom")) => // required not only for spark-streaming-kinesis-asl
        MergeStrategy.discard
      case "io.netty.versions.properties" :: xs =>
        MergeStrategy.discard
      case _ => MergeStrategy.deduplicate
    }
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}

assemblyMergeStrategy in assembly := {
  case old if old.endsWith("package-info.class") => MergeStrategy.first
  case old if old.endsWith("CSVParser$Token.class") => MergeStrategy.first
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}

// Drop these jars
assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set(
    "slf4j-api-1.6.4.jar",
    "httpclient-4.1.3.jar",
    "httpcore-4.1.3.jar",
    "xpp3_min-1.1.4c.jar",
    "solr-commons-csv-3.5.0.jar",
    "commons-beanutils-1.7.0.jar",
    "commons-beanutils-core-1.8.0.jar",
    "servlet-api-2.5.jar",
    "jsp-api-2.1.jar",
    "cassandra-driver-core-2.1.3-sources.jar",
    "minlog-1.2.jar",
    "jcl-over-slf4j-1.7.5.jar"
  )
  cp filter { jar => excludes(jar.data.getName) }
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

test in assembly := {}

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assembly <<= assembly.dependsOn(assemblyPackageScala)
