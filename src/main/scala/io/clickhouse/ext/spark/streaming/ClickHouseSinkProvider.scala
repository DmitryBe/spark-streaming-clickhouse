package io.clickhouse.ext.spark.streaming

import io.clickhouse.ext.ClickHouseUtils
import io.clickhouse.ext.tools.Retry
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

  def maxRetry: Int = 50
  def ignoreThrowable: (Throwable) => Boolean = (ex: Throwable) => ex match {
    case _: org.apache.http.NoHttpResponseException =>
      log.error(ex.getMessage)
      false
    case _: org.apache.http.conn.HttpHostConnectException =>
      log.error(ex.getMessage)
      false
    case rex: java.lang.RuntimeException =>
      rex.getCause match {
        case _: ru.yandex.clickhouse.except.ClickHouseException =>
          log.error(rex.getMessage)
          false // retry
        case _ =>
          log.error(rex.getMessage)
          true
      }
    case _ => true
  }

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
    log.info("Create new table sql:")
    log.info(createTableSql)

    val connection = ClickHouseUtils.createConnection(getConnectionString())
    try{
      connection.createStatement().execute(createTableSql)
    }finally {
      connection.close()
      log.info(s"ClickHouse table ${dbName}.${_tableName} created")
    }

    log.info("Creating ClickHouse sink")
    new ClickHouseSink[T](dbName, _tableName, eventDateColumnName)(getConnectionString)(partitionFunc)(maxRetry, ignoreThrowable)
  }

  def getConnectionString(): (String, Int) = clickHouseServers.head

}
