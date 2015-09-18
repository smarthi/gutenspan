package gutenspan

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.RemoteCacheManager

import java.util.Properties
import java.io._

object ImportBooks {
  def main(args: Array[String]) {
    if(args.length < 2) {
      println("Usage: ImportBooks master bookFile")
      System.exit(1)
    }
    
    val master = args(0)
    val flname = args(1)

    val reader = new BufferedReader(
      new FileReader(flname))

    val infinispanProperties = new Properties
    infinispanProperties.put("infinispan.client.hotrod.server_list", master)
    val config = new ConfigurationBuilder().withProperties(infinispanProperties).build()
    println("creating cache manager")
    val remoteCacheManager = new RemoteCacheManager(config)
    println("getting cache")
    val cache = remoteCacheManager.getCache[(String, Int), String]("books")
    println("Houston, we have a cache!")
    
    var line = reader.readLine()
    var i = 1
    while(line != null) {
      if(i % 1000 == 0) {
        println(i + " " + line)
      }

      cache.put((flname, i), line)

      line = reader.readLine()
      i += 1
    }

    println("Entries in cache: " + cache.size())
    
    reader.close()
  }
}
