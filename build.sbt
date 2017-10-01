name := "twitter-streaming"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.3"

        