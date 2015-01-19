/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {

	val csvLinesWithHeaders = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/spend/*")
	val header = csvLinesWithHeaders.first
	val csvLines=csvLinesWithHeaders.filter(line => line != header)
	final val IndexPrice = 4 // Position of the price column in the source CSV file. 

	// Split once only, and trim to remove leading and trailing invisible characters, white spaces, etc. 
	val cleanLines = csvLines.map(_.replace("\"","").replace(" , ","\t").split("\t").map(_.trim)).filter(_.isDefinedAt(IndexPrice))


	final val ForbiddenKeywords = Set("UNCLASSIFIED","UNCATEGORIZED", "NOT MAPPED TO BU") // Add undesirable keywords to this set.

	import scala.util.Try 
	def parseDouble(s: String): Double = Try(s.toDouble).getOrElse(0.0)

	// Filter early to remove empty and undesirable keywords:
	def extractKeywords(line: Seq[String]): Seq[String] = line.map(_.toUpperCase).filter(_.isDefinedAt(IndexPrice)).patch(IndexPrice, Nil, 1).filterNot(_.isEmpty).filterNot(ForbiddenKeywords.contains(_))

	import org.apache.spark.rdd.RDD
	val keywordsAndPrice: RDD[(Seq[String], Double)] = cleanLines.map(line => (extractKeywords(line).map(_.toUpperCase), parseDouble(line(IndexPrice))))
	val keywordAndPrice: RDD[(String, Double)] = keywordsAndPrice.flatMap { case(keywords, price) => keywords.map(keyword => (keyword,price)) }

	val results = keywordAndPrice.reduceByKey((a,b) => parseDouble(a.toString)/1000000 + parseDouble(b.toString)/1000000).sortBy(_._2,false).map(a => a._1 + "\t" + a._2)

	results.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/result/spend2")
	}
}


