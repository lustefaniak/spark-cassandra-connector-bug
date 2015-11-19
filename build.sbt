lazy val root = (project in file(".")).settings(
  name := "spark-cassandra-connector-bug",
  version := "1.0.0",
  scalaVersion := "2.11.7",
  fork := true,
  fork in Test := true,
  libraryDependencies ++= {
    val sparkVersion = "1.5.1"
    val sparkCassandraConnectorVersion = "1.5.0-M2"
    Seq(
      "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion
    )
  }
)

cancelable in Global := true
