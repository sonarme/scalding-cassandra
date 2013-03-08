package com.sonar.scalding.cassandra

import cascading.tap.{SinkMode, Tap}
import com.twitter.scalding._
import com.twitter.scalding.Hdfs
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.scheme.Scheme

/**
 * Scalding Cassandra Source for reading and writing.
 * Taking the best from
 * https://github.com/pingles/cascading.cassandra,
 * https://github.com/Cascading/maple,
 * https://github.com/amimimor/scalding/blob/develop/src/main/scala/com/twitter/scalding/HBaseSource.scala,
 * and cleaning up
 * @param rpcHost the host (or multiple, comma-separated) where the ring describe happens
 * @param rpcPort port for the host
 * @param keyspaceName the keyspace
 * @param columnFamilyName the column family for the source
 * @param scheme a narrow or wide column scheme
 * @param additionalConfig additional hadoop configuration entries
 * @author Ben Kempe
 */
case class CassandraSource(
                                  rpcHost: String, rpcPort: Int = 9160,
                                  keyspaceName: String,
                                  columnFamilyName: String,
                                  scheme: CassandraScheme,
                                  additionalConfig: Map[String, String] = Map.empty) extends Source {

    override val hdfsScheme = scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

    override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {

        val cassScheme = mode match {
            case Hdfs(strict, config) => hdfsScheme.asInstanceOf[CassandraScheme]
            case notImplemented => throw new RuntimeException("not implemented: " + notImplemented)
        }
        val sinkMode = readOrWrite match {
            case Read => SinkMode.KEEP
            case Write => SinkMode.UPDATE
        }
        new CassandraTap(rpcHost, rpcPort, keyspaceName, columnFamilyName, cassScheme, sinkMode, additionalConfig: Map[String, String])
    }
}
