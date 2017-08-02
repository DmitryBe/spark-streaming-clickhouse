# Spark structured streaming Clickhouse sink

Dump Spark structured streaming output to Yandex ClickHouse OLAP

## Quick start

Run ClickHouse server (local, docker)

`docker run -it -p 8123:8123 -p 9000:9000 --name clickhouse yandex/clickhouse-server`

Run ClickHouse client

`docker run -it --net=host --rm yandex/clickhouse-client`

Create ClickHouse databases

```sql
CREATE DATABASES IF NOT EXISTS db01
SHOW DATABASES
```

Create a project, define Spark structured streaming sink for ClickHouse

```scala
// input events
case class Event(word: String, timestamp: Timestamp)

// stream internal state
case class State(c: Int)

// stream output
case class StateUpdate(updateTimestamp: Timestamp, word: String, c: Int)

// clickhouse sink
class ClickHouseStateUpdatesSinkProvider extends ClickHouseSinkProvider[StateUpdate]{
  override def clickHouseServers: Seq[(String, Int)] = Seq(("localhost", 8123))
  override def dbName: String = "db01"
  override def tableName = Some("stateUpdates")
  override def eventDateColumnName: String = "eventDate"
  override def indexColumns: Seq[String] = Seq("word")
  override def partitionFunc: (Row) => Date =
    (row) => {
      // use event timestamp as partition key
      new java.sql.Date(row.getAs[Timestamp](0).getTime)
      // use current
      //new java.sql.Date(Calendar.getInstance().getTimeInMillis())
    }
}
```

Run `nc -lk 9999`

Describe Spark structured stream and start query

```scala
// spark session
val spark = SparkSession
    .builder
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "./spark-checkpoints")
    .appName("streaming-test")
    .getOrCreate()

import spark.implicits._

val host = "localhost"
val port = "9999"

// define socket source
val lines = spark.readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .option("includeTimestamp", true)
    .load()

// transform input data to stream of events
val events = lines
    .as[(String, Timestamp)]
    .flatMap { case (line, timestamp) =>
    line.split(" ").map(word => Event(word = word, timestamp))
    }

println(s"Events schema:")
events.printSchema()

// statefull transformation: word => Iterator[Event] => Iterator[StateUpdate]
val stateStream = events.groupByKey((x) => x.word)
    .flatMapGroupsWithState[State, StateUpdate](OutputMode.Append(), GroupStateTimeout.NoTimeout())((key, iter, state) => {

    // get / create new state
    val wState = state.getOption.getOrElse(State(0))
    val count = wState.c + iter.length

    // update state
    state.update(State(count))

    // output: Iterator[StateUpdate]
    List(
        StateUpdate(new Timestamp(Calendar.getInstance().getTimeInMillis), key, count)
    ).toIterator
    })

val query = stateStream.writeStream
    .outputMode("append")
    .format("ClickHouseEventsSinkProvider") // clickhouse sink
    //.format("console")
    .start()

query.awaitTermination()

```
