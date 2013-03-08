package com.sonar.scalding.cassandra

import collection.JavaConversions._
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple._
import cascading.util.Util
import org.apache.cassandra.thrift._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import me.prettyprint.cassandra.serializers.TypeInferringSerializer
import cascading.scheme.{SourceCall, SinkCall, Scheme}
import java.nio.ByteBuffer
import org.apache.cassandra.dht.RandomPartitioner
import CassandraTap._
import cascading.flow.FlowProcess
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import java.{lang => jl, util => ju}
import ju.UUID
import org.apache.cassandra.db.IColumn
import cascading.tuple.Fields
import org.apache.cassandra.hadoop._
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import cascading.flow.hadoop.HadoopFlowProcess

/**
 * @author Ben Kempe
 */
class CassandraTap(
                          val host: String,
                          val rpcPort: Int = 9160,
                          keyspace: String,
                          columnFamilyName: String,
                          val scheme: CassandraScheme,
                          sinkMode: SinkMode,
                          val additionalConfig: Map[String, String]
                          )
        extends Tap[JobConf, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]], OutputCollector[ByteBuffer, ju.List[Mutation]]](scheme, sinkMode) {
    require(host != null)
    val pathUUID = UUID.randomUUID.toString


    override def sourceConfInit(flowProcess: FlowProcess[JobConf], conf: JobConf) {
        FileInputFormat.addInputPaths(conf, getPath.toString)
        conf.setInputFormat(classOf[ColumnFamilyInputFormat])
        ConfigHelper.setInputPartitioner(conf, classOf[RandomPartitioner].getCanonicalName)
        ConfigHelper.setInputInitialAddress(conf, host)
        ConfigHelper.setInputRpcPort(conf, rpcPort.toString)
        ConfigHelper.setInputColumnFamily(conf, keyspace, columnFamilyName, scheme.wide)
        commonInit(flowProcess, conf)
        super.sourceConfInit(flowProcess, conf)
    }

    override def sinkConfInit(flowProcess: FlowProcess[JobConf], conf: JobConf) {
        super.sinkConfInit(flowProcess, conf)
        conf.setOutputFormat(classOf[ColumnFamilyOutputFormat])
        ConfigHelper.setOutputPartitioner(conf, classOf[RandomPartitioner].getCanonicalName)
        ConfigHelper.setOutputInitialAddress(conf, host)
        ConfigHelper.setOutputRpcPort(conf, rpcPort.toString)
        ConfigHelper.setOutputColumnFamily(conf, keyspace, columnFamilyName)
        conf.setOutputKeyClass(classOf[ByteBuffer])
        conf.setOutputValueClass(classOf[Mutation])
        commonInit(flowProcess, conf)
    }

    def commonInit(flowProcess: FlowProcess[JobConf], conf: JobConf) {
        additionalConfig foreach {
            case (k, v) => conf.set(k, v)
        }
    }

    protected def getStringURI = UriScheme + "://" + host + ":" + rpcPort + "/" + keyspace + "/" + columnFamilyName

    def getPath: Path = new Path(pathUUID)

    override def toString: String = getClass.getSimpleName + "[\"" + UriScheme + "\"]" + "[\"" + Util.sanitizeUrl(getStringURI) + "\"]" + "[\"" + pathUUID + "\"]"

    override def equals(other: Any) =
        other match {
            case that: CassandraTap =>
                super.equals(that) &&
                        (that canEqual this) &&
                        scheme == that.scheme &&
                        getStringURI == that.getStringURI

            case _ => false
        }

    def canEqual(other: Any) = other.isInstanceOf[CassandraTap]

    override def hashCode = 41 * (41 * super.hashCode + scheme.hashCode()) + getStringURI.hashCode()

    def getIdentifier = pathUUID

    def openForRead(flowProcess: FlowProcess[JobConf], input: RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]]) =
        new HadoopTupleEntrySchemeIterator(flowProcess, this, input)

    def openForWrite(flowProcess: FlowProcess[JobConf], outputCollector: OutputCollector[ByteBuffer, ju.List[Mutation]]) =
        new CassandraTapCollector(flowProcess, scheme, outputCollector, getIdentifier)

    def createResource(p1: JobConf) =
        throw new UnsupportedOperationException("createResource unsupported with Cassandra.")

    def deleteResource(p1: JobConf) =
        throw new UnsupportedOperationException("deleteResource unsupported with Cassandra.")

    def resourceExists(p1: JobConf) =
        throw new UnsupportedOperationException("resourceExists unsupported with Cassandra.")

    def getModifiedTime(p1: JobConf) = System.currentTimeMillis()


}

object CassandraTap {
    val UriScheme = "cassandra"
    type CassandraData = Pair[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]]

}

/**
 * Mapping wide cassandra rows directly to tuples with (rowKey, columnName, columnValue)
 * @param keyField the tuple field name for the row key
 * @param nameValueFields the tuple field names for the column list
 * @param includeTimestamp whether to include the column timestamp
 */
case class WideRowScheme(keyField: Fields, nameValueFields: Fields = Fields.NONE, includeTimestamp: Boolean = false)
        extends CassandraScheme(keyField, nameValueFields, wide = true) {

    def predicate = new SlicePredicate

    def extractValue(result: Tuple, columns: ju.SortedMap[ByteBuffer, IColumn]) {
        columns.values().foreach {
            column =>
                result.add(column.name())
                result.add(column.value())
                if (includeTimestamp) result.add(column.timestamp())
        }
    }

    def createColumns(tupleEntry: TupleEntry) =
        for (group <- tupleEntry.getTuple.tail.grouped(2).toIterable) yield {
            val Seq(name, value) = group.toSeq
            serialize(name) -> serialize(value)
        }
}

/**
 * Mapping a narrow cassandra row directly to tuples
 * @param keyField the tuple field name for the row key
 * @param valueFields the tuple field names for the column values
 * @param columnNames the cassandra columns to retrieve
 */
case class NarrowRowScheme(keyField: Fields, valueFields: Fields, columnNames: Seq[String] = Seq.empty[String])
        extends CassandraScheme(keyField, valueFields, wide = false) {


    def columnNamesToUse: Iterable[_] =
        if (columnNames.isEmpty) valueFields.asInstanceOf[jl.Iterable[jl.Comparable[Object]]] else columnNames


    def predicate = new SlicePredicate setColumn_names (columnNamesToUse map {
        n: Any => serialize(n)
    } toList)

    def extractValue(result: Tuple, value: ju.SortedMap[ByteBuffer, IColumn]) {
        columnValues(value, columnNamesToUse) foreach (result.add)
    }

    def columnValues(values: ju.SortedMap[ByteBuffer, IColumn], names: Iterable[_]) =
        for (name <- names) yield {
            val column = values.get(serialize(name))
            if (column == null) null else column.value()
        }

    def createColumns(tupleEntry: TupleEntry) =
        for ((name, idx) <- valueFields.zipWithIndex) yield {
            val value = tupleEntry.getObject(name)
            serialize(getName(idx)) -> serialize(value)
        }

    private def getName(fieldPos: Int) = if (columnNames.isEmpty) valueFields.get(fieldPos) else columnNames(fieldPos)
}

abstract class CassandraScheme(keyField: Fields, valueFields: Fields, val wide: Boolean)
        extends Scheme[JobConf, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]], OutputCollector[ByteBuffer, ju.List[Mutation]], CassandraData, CassandraData] {
    require(keyField.size() == 1, "may only have one key field, found: " + keyField.print())
    val fields = Fields.join(keyField, valueFields)
    setSourceFields(fields)

    def serialize(obj: Any) = TypeInferringSerializer.get().toByteBuffer(obj)

    def extractValue(result: Tuple, value: ju.SortedMap[ByteBuffer, IColumn])

    def predicate: SlicePredicate

    def sourceConfInit(p1: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]], OutputCollector[ByteBuffer, ju.List[Mutation]]], jobConf: JobConf) {
        ConfigHelper.setInputSlicePredicate(jobConf, predicate)
    }

    def source(p1: FlowProcess[JobConf], sourceCall: SourceCall[CassandraData, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]]]) = {
        val result = new Tuple()
        val (key, value) = sourceCall.getContext
        val hasNext = sourceCall.getInput.next(key, value)
        if (hasNext) {
            result.add(key)
            extractValue(result, value)
            sourceCall.getIncomingEntry.setTuple(result)
            true
        } else false
    }

    override def sourcePrepare(flowProcess: FlowProcess[JobConf], sourceCall: SourceCall[CassandraData, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]]]) {
        sourceCall.setContext(sourceCall.getInput.createKey() -> sourceCall.getInput.createValue())
    }

    override def sourceCleanup(flowProcess: FlowProcess[JobConf], sourceCall: SourceCall[CassandraData, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]]]) {
        sourceCall.setContext(null)
    }

    def createColumns(tupleEntry: TupleEntry): Iterable[(ByteBuffer, ByteBuffer)]

    def sink(p1: FlowProcess[JobConf], sinkCall: SinkCall[CassandraData, OutputCollector[ByteBuffer, ju.List[Mutation]]]) {
        val outputCollector = sinkCall.getOutput
        val tupleEntry = sinkCall.getOutgoingEntry
        val tuple = sinkCall.getOutgoingEntry.getTuple
        val keyBuffer = serialize(tuple.getObject(0))
        val mutations = createColumns(tupleEntry) map {
            case (name, value) => createColumnPutMutation(serialize(name), serialize(value))
        }
        outputCollector.collect(keyBuffer, mutations.toList)
    }

    // currently using the default clock resolution from hector
    def createClock() = CassandraHostConfigurator.DEF_CLOCK_RESOLUTION.createClock()

    def createColumnPutMutation(name: ByteBuffer, value: ByteBuffer) = {
        val column = new Column(name) setValue (value) setTimestamp (createClock())
        val columnOrSuperColumn = new ColumnOrSuperColumn setColumn (column)
        new Mutation setColumn_or_supercolumn (columnOrSuperColumn)
    }

    def sinkConfInit(flowProcess: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[ByteBuffer, ju.SortedMap[ByteBuffer, IColumn]], OutputCollector[ByteBuffer, ju.List[Mutation]]], conf: JobConf) {
    }

}

class CassandraTapCollector(flowProcess: FlowProcess[JobConf], scheme: CassandraScheme, outputCollector: OutputCollector[ByteBuffer, ju.List[Mutation]], val identifier: String)
        extends TupleEntrySchemeCollector[JobConf, Any](flowProcess, scheme) with OutputCollector[ByteBuffer, ju.List[Mutation]] {
    var writer: RecordWriter[ByteBuffer, ju.List[Mutation]] = _
    val conf = flowProcess.getConfigCopy
    var prepared = false

    def collect(key: ByteBuffer, value: ju.List[Mutation]) {
        flowProcess match {
            case hadoopFlowProcess: HadoopFlowProcess => hadoopFlowProcess.getReporter.progress()
            case _ =>
        }
        writer.write(key, value)
    }

    override def close() {

        try {
            if (prepared)
                writer.close(Reporter.NULL)
        } finally {
            super.close()
        }
    }

    override def prepare() {
        val outputFormat = new ColumnFamilyOutputFormat
        writer = outputFormat.getRecordWriter(null, conf, identifier, Reporter.NULL)
        sinkCall.setOutput(this)
        prepared = true
        super.prepare()
    }
}
