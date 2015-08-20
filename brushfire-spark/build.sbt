name := "brushfire-spark"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient,
  Deps.sparkCore,
  Deps.algebirdSpark
)

// mainClass := Some("com.twitter.scalding.Tool")

Publish.settings

MakeJar.settings

