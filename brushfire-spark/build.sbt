name := "brushfire-spark"

resolvers += Resolvers.conjars

libraryDependencies ++= Seq(
  Deps.hadoopClient,
  Deps.sparkCore
)

// mainClass := Some("com.twitter.scalding.Tool")

Publish.settings

MakeJar.settings

