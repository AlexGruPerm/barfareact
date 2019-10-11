name := "barfareact"
version := "1.0"

resolvers += Resolver.sonatypeRepo("snapshots")
scalaVersion := "2.13.0"

libraryDependencies += "com.datastax.oss" % "java-driver-core" % "4.0.1"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies +="com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
//libraryDependencies +="org.scala-lang" % "scala-library" % "2.12.4"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.13.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "logback.xml" => MergeStrategy.last
  case "resources/logback.xml" => MergeStrategy.last
  case "resources/application.conf" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

assemblyJarName in assembly :="barfareact.jar"
mainClass in (Compile, packageBin) := Some("barfareact.MainBarFaCalc")
mainClass in (Compile, run) := Some("barfareact.MainBarFaCalc")

