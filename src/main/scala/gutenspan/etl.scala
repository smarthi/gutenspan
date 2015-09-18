package gutenspan

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.RemoteCacheManager

import org.infinispan.spark._

import scala.collection.JavaConversions._

object ETL {
  def main(args: Array[String]) {
    if(args.length < 1) {
      println("Usage: ImportBooks master")
      System.exit(1)
    }
    
    val master = args(0)

    val srcProp = new Properties
    srcProp.put("infinispan.client.hotrod.server_list", master)
    srcProp.put(InfinispanRDD.CacheName, "books")

    val destProp = new Properties
    destProp.put("infinispan.client.hotrod.server_list", master)
    destProp.put(InfinispanRDD.CacheName, "words")
    val config = new ConfigurationBuilder().withProperties(destProp).build()

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("etl")
    val sc = new SparkContext(conf)

    val bookLines =
      new InfinispanRDD[(String, Int), String](sc,
                                               configuration=srcProp)

    println(bookLines.count() + " lines of text")

    val filenameTitles = bookLines.filter {
      case ((filename, lineNumber), line) =>
        line.startsWith("Title:")
    }.map {
      case ((filename, lineNumber), line) =>
        (filename, line.substring("Title: ".length))
    }.collectAsMap()

    println(filenameTitles.size() + " books")

    for( (filename, title) <- filenameTitles) {
      println("Book: " + title)
    }

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

    wordCounts.writeToInfinispan(destProp)

    println("Entries in cache after: " + cache.size())

    sc.stop()
  }
}
