import org.apache.spark.sql.SparkSession
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import WordCountJob._

@RunWith(classOf[JUnitRunner])
object WordCountSpec extends SpecificationWithJUnit {

  "WordCountSpec tests" should {

    implicit val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("WordCountJobSpec")
      .getOrCreate()
    import sparkSession.implicits._

    "test WordCountJob.filterHeaders" in {

      "test sample case" in {
        val testData = List(
          """Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>""",
          """Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)""",
          """From: phillip.allen@enron.com""",
          """To: tim.belden@enron.com""",
          """Subject: """,
          """Mime-Version: 1.0""",
          """Content-Type: text/plain; charset=us-ascii""",
          """Content-Transfer-Encoding: 7bit""",
          """X-From: Phillip K Allen""",
          """X-To: Tim Belden <Tim Belden/Enron@EnronXGate>""",
          """X-cc: """,
          """X-bcc: """,
          """X-Folder: \Phillip_Allen_Jan2002_1\Allen, Phillip K.\'Sent Mail""",
          """X-Origin: Allen-P""",
          """X-FileName: pallen (Non-Privileged).pst""",
          """""",
          """word1""",
          """word222""",
          """word333 word444 aaaa""",
          """""",
          """morewords aaaa   aaaa""",
          """andmorewords word444"""
        )
        val ds = sparkSession.createDataset(testData).as[String]

        // the initial lines should be 22
        ds.count() mustEqual 22

        // filter out headers
        val filtered = filterHeaders(ds)
        filtered.count() mustEqual 5

        val actual: List[String] = filtered.collect().toList
        val expect: List[String] = List("word1", "word222", "word333 word444 aaaa", "morewords aaaa   aaaa",
          "andmorewords word444")
        actual must containTheSameElementsAs(expect)
      }

      "testcase empty strings" in {
        val testData = List("", "", "")
        val expect = List.empty[String]
        filterHeaders(sparkSession.createDataset(testData)).as[String].collect()
          .toList must containTheSameElementsAs(expect)
      }

      "testcase empty list" in {
        val testData = List.empty[String]
        val expect = List.empty[String]
        filterHeaders(sparkSession.createDataset(testData)).as[String].collect()
          .toList must containTheSameElementsAs(expect)
      }

      "testcase only headers" in {
        val testData = List(
          """Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>""",
          """Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)""",
          """From: phillip.allen@enron.com"""
        )
        val expect = List.empty[String]
        filterHeaders(sparkSession.createDataset(testData)).as[String].collect()
          .toList must containTheSameElementsAs(expect)
      }

      "testcase only valid text" in {
        val testData = List(
          """this is a valid line""",
          """this is also a valid line"""
        )
        filterHeaders(sparkSession.createDataset(testData)).as[String].collect()
          .toList must containTheSameElementsAs(testData)
      }

    }

    "test WordCountJob.generateWords" in {

      "test sample case" in {
        val testData = List("word1", "word222", "word333 word444 aaaa", "morewords aaaa     aaaa",
          "andmorewords word444")
        val expect = List("word1", "word222", "word333", "word444", "aaaa", "morewords", "aaaa", "aaaa",
          "andmorewords", "word444")
        val ds = sparkSession.createDataset(testData).as[String]
        generateWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

      "testcase white space strings" in {
        val testData = List(" ", "     ")
        val expect = List.empty[String]
        val ds = sparkSession.createDataset(testData).as[String]
        generateWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

      "testcase empty strings" in {
        val testData = List("", "")
        val expect = List("", "")
        val ds = sparkSession.createDataset(testData).as[String]
        generateWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

    }

    "test WordCountJob.countWords" in {

      "test sample case" in {
        val testData = List("word1", "word222", "word333", "word444", "aaaa", "morewords", "aaaa", "aaaa",
          "andmorewords", "word444")
        val expect = List(
          ("word1", 1),
          ("word222", 1),
          ("word333", 1),
          ("word444", 2),
          ("aaaa", 3),
          ("morewords", 1),
          ("andmorewords", 1)
        )
        val ds = sparkSession.createDataset(testData).as[String]
        countWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

      "testcase empty strings" in {
        val testData = List("", "")
        val expect = List(("", 2))
        val ds = sparkSession.createDataset(testData).as[String]
        countWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

      "testcase empty ds" in {
        val testData = List.empty[String]
        val expect = List.empty[String]
        val ds = sparkSession.createDataset(testData).as[String]
        countWords(ds).collect().toList must containTheSameElementsAs(expect)
      }

    }

    "test WordCountJob.filterLower" in {

      "testcase sample" in {
        val testData = List(("opla", 20L), ("big", 10L), ("small", 9L))
        val expect = List(("opla", 20L), ("big", 10L))
        val ds = sparkSession.createDataset(testData).as[(String, Long)]
        filterLower(ds).collect().toList must containTheSameElementsAs(expect)
      }

    }

  } // end of should

}
