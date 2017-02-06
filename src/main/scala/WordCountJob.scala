import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.JavaConverters._

/**
  * Welcome to WordCount, a "Hello World" for big data!
  *
  * author: Martin Klosi
  */
object WordCountJob {

  /**
    * Main method. This is where the magic begins.
    * @param args - command-line arguments for this job
    */
  def main(args: Array[String]): Unit = {

    val config = makeCmdLineConfig(args)

    implicit val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("WordCountJob")
      .getOrCreate()
    import sparkSession.implicits._

    // read corpus from input location
    val ds = sparkSession.read.textFile(config.getString("input-path"))
    // filter out headers
    val filtered = filterHeaders(ds)
    // extract individual words from corpus
    val words = generateWords(filtered)
    // get word counts
    val counts = countWords(words)
    // filter out any words with less than 10 count
    val filteredLower = filterLower(counts)
    // sort words by count, descending
    val sorted = filteredLower.sort($"count(1)".desc)

    sorted
      // coalesce partitions into a single partition, so we end up with just one output file
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .save(config.getString("output-path"))

  } // end of main

  /**
    * This method takes command line arguments and converts them into
    * a typesafe Config object, which makes it easier to work with custom
    * job configurations, like input and output
    * @param args - command line arguments
    * @return - typesafe Config object containing all the args as job configurations
    */
  private def makeCmdLineConfig(args: Array[String]): Config = {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
        "Invalid argument format, accepted format is (with -- being optional): --arg-name value"
      )
    }
    val keyValuePairs = args.grouped(2).map { pair =>
      val key = pair(0).dropWhile(_ == '-').trim
      key -> pair(1)
    }.toMap
    ConfigFactory.parseMap(keyValuePairs.asJava)
  }

  /**
    * This method filters out any header text lines.
    * @param ds - the dataset of rows that will be filtered.
    * @return - filtered dataset without header rows.
    */
  def filterHeaders(ds: Dataset[String]): Dataset[String] = {
    //noinspection ConvertibleToMethodValue
    ds.filter {
      line =>
        if (line.startsWith("Message-ID: ") ||
          line.startsWith("Date: ") ||
          line.startsWith("From: ") ||
          line.startsWith("To: ") ||
          line.startsWith("Subject: ") ||
          line.startsWith("Mime-Version: ") ||
          line.startsWith("Content-Type: ") ||
          line.startsWith("Content-Transfer-Encoding: ") ||
          line.startsWith("X-From: ") ||
          line.startsWith("X-To: ") ||
          line.startsWith("X-cc: ") ||
          line.startsWith("X-bcc: ") ||
          line.startsWith("X-Folder: ") ||
          line.startsWith("X-Origin: ") ||
          line.startsWith("X-FileName: ") ||
          line.startsWith(" ") ||
          line.isEmpty) {
          false
        } else {
          true
        }
    }
  }

  /**
    * This method splits all the row in the input dataset into infividual words.
    * The splitting is done on a white character.
    * @param ds - input dataset, where each record is a line of text.
    * @return - dataset, where each record is an individual word
    */
  def generateWords(ds: Dataset[String])(implicit sparkSession: SparkSession): Dataset[String] = {
    import sparkSession.implicits._
    ds.flatMap(value => value.split("\\s+"))
  }

  /**
    * This method groups all the words in the incoming dataset and count their occurrence.
    * @param ds - input dataset, where each record is a String word.
    * @return - a dataset, where each record is a tuple. First elem of tuple
    *         is a unique word, second elem is the count of this word in the corpus.
    */
  def countWords(ds: Dataset[String])(implicit sparkSession: SparkSession): Dataset[(String, Long)] = {
    import sparkSession.implicits._
    ds.groupByKey(_.toLowerCase()).count()
  }

  /**
    * This method filters out all the word that occur less than 10 time in the corpus.
    * @param ds - input dataset of tuples of unique words and their respective counts.
    * @return - a dataset that retains only words that appear at least 10 times in the doc corpus.
    */
  def filterLower(ds: Dataset[(String, Long)]): Dataset[(String, Long)] = {
    ds.filter(_._2 >= 10)
  }

}
