name := "gutenspan"

version := "0.1"

val SPARK_VERSION = "1.4.1"
val infinispanVersion = "8.0.1.Final"
val jcipAnnotationsVersion = "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.infinispan" % "infinispan-client-hotrod" % infinispanVersion

libraryDependencies += "net.jcip" % "jcip-annotations" % jcipAnnotationsVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % SPARK_VERSION % "provided"

libraryDependencies += "org.infinispan" %% "infinispan-spark" % "0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}
