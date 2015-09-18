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

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("etl")
    val sc = new SparkContext(conf)

    val srcProp = new Properties
    srcProp.put("infinispan.client.hotrod.server_list", master)
    srcProp.put(InfinispanRDD.CacheName, "books")
    val books = new InfinispanRDD[(String, Int), String](sc,
                               configuration=srcProp)

    println(books.count() + " lines of text")

    val words = books.flatMap {
      case ((filename, lineNumber), text) =>
        val words = text.toLowerCase()
          .filter { c => "abcdefghijklmnopqrstuvwxyz \t\n".contains(c) }
          .split(" ")

        words.map {
          w =>
          ((filename, w), 1)
        }
    }

    println(words.count() + " words")

    val wordCounts = words.reduceByKey(_+_)

    println(wordCounts.count() + " unique words")

    val destProp = new Properties
    destProp.put("infinispan.client.hotrod.server_list", master)
    destProp.put(InfinispanRDD.CacheName, "words")
    val config = new ConfigurationBuilder().withProperties(destProp).build()
    println("creating cache manager")
    val remoteCacheManager = new RemoteCacheManager(config)
    println("getting cache")
    val cache = remoteCacheManager.getCache[(String, String), Int]("words")
    println("Houston, we have a cache!")
    
    println("Entres in cache before: " + cache.size())

    words.writeToInfinispan(destProp)

    println("Entries in cache after: " + cache.size())

    sc.stop()
  }
}
