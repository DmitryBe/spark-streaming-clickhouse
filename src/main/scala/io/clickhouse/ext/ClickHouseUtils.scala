package io.clickhouse.ext

import java.sql.PreparedStatement
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}
import ru.yandex.clickhouse.settings.ClickHouseProperties

object ClickHouseUtils extends Serializable with Logging {

  def createConnection(hostPort: (String, Int)) = {
    val properties = new ClickHouseProperties()
    val dataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${hostPort._1}:${hostPort._2}", properties)
    dataSource.getConnection()
  }

  def createTableIfNotExistsSql(schema: StructType, dbName: String, tableName: String, eventDateColumnName: String, indexColumns: Seq[String]) = {

    val tableFieldsStr = schema.map{ f =>

      val fName = f.name
      val fType = f.dataType match {
        case org.apache.spark.sql.types.DataTypes.StringType => "String"
        case org.apache.spark.sql.types.DataTypes.IntegerType => "Int32"
        case org.apache.spark.sql.types.DataTypes.FloatType => "Float32"
        case org.apache.spark.sql.types.DataTypes.LongType => "Int64"
        case org.apache.spark.sql.types.DataTypes.BooleanType => "UInt8"
        case org.apache.spark.sql.types.DataTypes.DoubleType => "Float64"
        case org.apache.spark.sql.types.DataTypes.DateType => "DateTime"
        case org.apache.spark.sql.types.DataTypes.TimestampType => "DateTime"
        case x => throw new Exception(s"Unsupported type: ${x.toString}")
      }

      s"$fName $fType"

    }.mkString(", ")

    val createTableSql = s"""CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} ($eventDateColumnName Date, $tableFieldsStr) ENGINE = MergeTree($eventDateColumnName, (${indexColumns.mkString(",")}), 8192)"""

    // return
    createTableSql
  }

  def prepareInsertStatement(connection: ClickHouseConnection, dbName: String, tableName: String, partitionColumnName: String)
                            (schema: org.apache.spark.sql.types.StructType) = {

    val insertSql = createInsertStatmentSql(dbName, tableName, partitionColumnName)(schema)
    log.debug("ClickHouse insert sql:")
    log.debug(insertSql)
    connection.prepareStatement(insertSql)
  }

  def batchAdd(schema: org.apache.spark.sql.types.StructType, row: Row)
              (statement: PreparedStatement)
              (partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date)= {

    val partitionVal = partitionFunc(row)
    statement.setDate(1, partitionVal)

    schema.foreach{ f =>
      val fieldName = f.name
      val fieldIdx = schema.fieldIndex(fieldName)
      val fieldVal = row.get(fieldIdx)
      if(fieldVal != null)
        statement.setObject(fieldIdx + 2, fieldVal)
      else{
        val defVal = defaultNullValue(f.dataType, fieldVal)
        statement.setObject(fieldIdx + 2, defVal)
      }
    }

    // add into batch
    statement.addBatch()
  }

  private def createInsertStatmentSql(dbName: String, tableName: String, partitionColumnName: String)
                                     (schema: org.apache.spark.sql.types.StructType) = {

    val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def defaultNullValue(sparkType: org.apache.spark.sql.types.DataType, v: Any) = sparkType match {

    case DoubleType => 0
    case LongType => 0
    case FloatType => 0
    case IntegerType => 0
    case StringType => null
    case BooleanType => false
    case _ => null
  }

}
