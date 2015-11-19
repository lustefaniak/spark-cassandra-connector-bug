package buggy

import java.net.InetAddress
import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader.PrefetchingResultSetIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Main extends App {

  val cassandraConnector = CassandraConnector.apply(Set(InetAddress.getByName("localhost")))
  cassandraConnector.withSessionDo {
    session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS buggy WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
      session.execute(
        """
          |CREATE TABLE IF NOT EXISTS buggy.t (
          |    item_id uuid,
          |    some_key bigint,
          |    val bigint,
          |    PRIMARY KEY (item_id, some_key)
          |);
        """.stripMargin)


  }

  val sparkConfWithWorkaround = new SparkConf()
    .setMaster("local[*]")
    .setAppName("with-workaround")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.ui.enabled", "true")
    .set("spark.cassandra.connection.keep_alive_ms", "3600000")

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("broken")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.ui.enabled", "true")

  val elements = Seq.fill(10000)(UUID.randomUUID())

  println("Testing code with altered 'spark.cassandra.connection.keep_alive_ms' setting:")

  val scWorkaround = new SparkContext(sparkConfWithWorkaround)
  try {
    val rddWorkaround = scWorkaround.parallelize(elements)
    assert(doMappingInPartitions(rddWorkaround).count() == elements.size)
  } finally {
    scWorkaround.stop()
  }

  println("Testing code without altered 'spark.cassandra.connection.keep_alive_ms' setting:")


  val scBroken = new SparkContext(sparkConf)
  try {
    val rddBroken = scBroken.parallelize(elements)
    assert(doMappingInPartitions(rddBroken).count() == elements.size)
  } finally {
    scBroken.stop()
  }

  println("Done")

  def doMappingInPartitions(in: RDD[UUID]): RDD[(UUID, Seq[String])] = {
    val connector = CassandraConnector(in.context.getConf)

    in.mapPartitions {
      items =>
        connector.withSessionDo {
          session =>
            items.map {
              item =>
                val rs = session.execute("SELECT * FROM buggy.t WHERE item_id = ?", item)
                val reader = new PrefetchingResultSetIterator(rs, 1000)
                val rows = reader.map(_.getString("val"))
                (item, rows.toSeq)
            }
        }
    }
  }

  sys.exit(0)

}
