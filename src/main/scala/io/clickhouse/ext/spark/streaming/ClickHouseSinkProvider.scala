package io.clickhouse.ext.spark.streaming

import io.clickhouse.ext.ClickHouseUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.TypeTag

abstract class ClickHouseSinkProvider[T <: Product: ClassTag](implicit tag: TypeTag[T]) extends StreamSinkProvider with Serializable with Logging {

  def clickHouseServers: Seq[(String, Int)]
  def dbName: String
  def tableName: Option[String] = None
  def eventDateColumnName: String
  def indexColumns: Seq[String]
  def partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): ClickHouseSink[T] = {

    val typeEncoder = Encoders.product[T]
    val schema = typeEncoder.schema
    val _tableName = tableName.get //tableName.getOrElse(classOf[T].getName)

    val createTableSql = ClickHouseUtils.createTableIfNotExistsSql(
      schema,
      dbName,
      _tableName,
      eventDateColumnName,
      indexColumns
    )
    log.info("create new table sql:")
    log.info(createTableSql)

    val connection = ClickHouseUtils.createConnection(getConnectionString())
    try{
      connection.createStatement().execute(createTableSql)
    }finally {
      connection.close()
      log.info(s"ClickHouse table ${dbName}.${_tableName} created")
    }

    log.info("Creating ClickHouse sink")
    new ClickHouseSink[T](dbName, _tableName, eventDateColumnName)(getConnectionString)(partitionFunc)
  }

  def getConnectionString(): (String, Int) = clickHouseServers.head

}
