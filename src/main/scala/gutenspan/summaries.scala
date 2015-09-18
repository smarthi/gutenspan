package gutenspan

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.RemoteCacheManager

import org.infinispan.spark._

import scala.collection.JavaConversions._

object Summarize {
  def main(args: Array[String]) {
    if(args.length < 1) {
      println("Usage: ImportBooks master")
      System.exit(1)
    }
    
    val master = args(0)

    val srcProp = new Properties
    srcProp.put("infinispan.client.hotrod.server_list", master)
    srcProp.put(InfinispanRDD.CacheName, "words")

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("summaries")
    val sc = new SparkContext(conf)

    val bookWords =
      new InfinispanRDD[(String, String), Int](sc,
                                               configuration=srcProp)

    val bookWordCounts = bookWords.map {
      case ((title, word), count) =>
        (title, count)
    }.reduceByKey(_+_)
    .collectAsMap

    for( (title, count) <- bookWordCounts) {
      println("Book: " + title + ", Words: " + count)
    }

    val docFreq = bookWords.keys
    .map { 
      case (title, word) =>
        (word, 1)
    }.reduceByKey(_+_)
    .collectAsMap

    val nDocs = bookWordCounts.size()

    val bookTfIdf = bookWords.map {
      case ((title, word), count) =>
        // words in all documents will be penalized as 0
        val idf = Math.log( nDocs / docFreq(word) )
        (title, (word, count * idf) )
    }

    val bookTopWords = bookTfIdf.groupByKey()
    .map {
      case (title, wordIter) =>
        val top = wordIter.toSeq
          .sortBy { case (word, tfIdf) => -1.0 * tfIdf  }
          .take(10)
        (title, top)
    }.collectAsMap

    for( (title, topWords) <- bookTopWords) {
      for( (word, tfIdf) <- topWords) {
        println("Book: " + title + ", word: " + word + ", tfIdf: " + tfIdf)
      }
    }
    

    /*
    val bookTitles = bookLines.map {
      case ((filename, lineNumber), line) =>
        ((filenameTitles(filename), lineNumber), line)
    }

    val words = bookTitles.flatMap {
      case ((title, lineNumber), line) =>
        val words = line.toLowerCase()
          .replaceAll("\t", " ")
          .replaceAll("\n", " ")
          .filter { c => "abcdefghijklmnopqrstuvwxyz ".contains(c) }
          .split(" ")

        words.map {
          w =>
          ((title, w), 1)
        }
    }

    println(words.count() + " words")

    val wordCounts = words.reduceByKey(_+_)

    println(wordCounts.count() + " unique words/book")

    println("creating cache manager")
    val remoteCacheManager = new RemoteCacheManager(config)
    println("getting cache")
    val cache = remoteCacheManager.getCache[(String, String), Int]("words")
    println("Houston, we have a cache!")
    
    println("Entres in cache before: " + cache.size())

    words.writeToInfinispan(destProp)

    println("Entries in cache after: " + cache.size())
    */
    sc.stop()
  }
}
