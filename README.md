# scalding-cassandra

[Cassandra](http://cassandra.apache.org/) source for [Scalding](https://github.com/twitter/scalding/).

Inspired by:
* https://github.com/pingles/cascading.cassandra
* https://github.com/Cascading/maple
* https://github.com/amimimor/scalding/blob/develop/src/main/scala/com/twitter/scalding/HBaseSource.scala

## Install

Build the Maven project and use the dependency
```
<dependency>
 <groupId>com.sonar</groupId>
 <artifactId>scalding-cassandra</artifactId>
 <version>1.0-SNAPSHOT</version>
</dependency>
```

Beware of:
[CASSANDRA-4834](https://issues.apache.org/jira/browse/CASSANDRA-4834)

## Examples

### Tap

#### NarrowRowScheme

```scala
    CassandraSource(
        rpcHost = "yourCassandraIp",
        keyspaceName = "yourKeyspace",
        columnFamilyName = "SomeColumnFamily",
        scheme = NarrowRowScheme(
            keyField = 'rowKeyBuffer,
            nameFields = ('someTupleField1, 'someTupleField2),
            columnNames = List("someColumnName1", "someColumnName2"))
    ).map(('rowKeyBuffer, 'someTupleField1, 'someTupleField2) ->('rowKey, 'field1, 'field2)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            val rowKeyDes = StringSerializer.get().fromByteBuffer(in._1)
            val field1 = Option(in._2).map(StringSerializer.get().fromByteBuffer)
            val field2 = Option(in._3).map(StringSerializer.get().fromByteBuffer)
            (rowKeyDes, field1, field2)
        }
    } // ...
```

#### WideRowScheme

```scala
    CassandraSource(
        rpcHost = "yourCassandraIp",
        keyspaceName = "yourKeyspace",
        columnFamilyName = "SomeColumnFamily",
        scheme = WideRowScheme(keyField = 'rowKeyBuffer,
            nameField = ('columnNameBuffer, 'columnValueBuffer))
    ).map(('rowKeyBuffer, 'columnNameBuffer, 'columnValueBuffer) ->('rowKey, 'columnName, 'columnValue)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) => {
            val rowKey = StringSerializer.get().fromByteBuffer(in._1)
			      val columnName = StringSerializer.get().fromByteBuffer(in._2)
			      val columnValue = StringSerializer.get().fromByteBuffer(in._3)
            (rowKey, columnName, columnValue)
        }
    } //...
```	
	

### Sink

Make sure that tuples reaching the sink are already serialized into ByteBuffers or are using a class registered in SerializerTypeInferer (String, Long, Date, etc.)

#### NarrowRowScheme

```scala
    .write(
        CassandraSource(
            rpcHost = "yourCassandraIp",
            keyspaceName = "yourKeyspace",
            columnFamilyName = "SomeColumnFamily",
            scheme = NarrowRowScheme(
            keyField = 'rowKeyBuffer,
            nameFields = ('someTupleField1, 'someTupleField2),
            columnNames = List("someColumnName1", "someColumnName2"))
        )
    )
```

#### WideRowScheme

Tuples written with a WideRowScheme MUST have the tuple format (rowKey, columnName1, columnValue1, ..., columnNameN, columnValueN)

```scala
    // ...
    .write(
        CassandraSource(
            rpcHost = "yourCassandraIp",
            keyspaceName = "yourKeyspace",
            columnFamilyName = "SomeColumnFamily",
            scheme = WideRowScheme(keyField = 'sonarId)
        )
    )
```

## TODO

* Simplify serialization/deserialization of ByteBuffers
* Use BulkOutputFormat

## Author

* [Ben Kempe](http://github.com/bkempe) ([@ben_0815](http://twitter.com/ben_0815))
