package huajunge.github.io.spatialman.utils

import com.esri.core.geometry._
import huajunge.github.io.spatialman.client.Constants.{DEFAULT_CF, GEOM}
import huajunge.github.io.spatialman.index.LocSIndex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FilterList, MultiRowRangeFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import java.io.IOException

class HClient @throws[IOException]
(var tableName: String, var g: Short, var alpha: Int, var beta: Int, var locSIndex: LocSIndex) {
  private var admin: Admin = _
  private var hTable: Table = _
  private var connection: Connection = _
  val conf: Configuration = HBaseConfiguration.create
  this.connection = ConnectionFactory.createConnection(conf)
  this.admin = connection.getAdmin
  val table: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
  if (!admin.tableExists(table.getTableName)) create()
  this.hTable = this.connection.getTable(TableName.valueOf(tableName))


  def this(tableName: String, g: Short, alpha: Int, beta: Int) {
    this(tableName, g, alpha, beta, LocSIndex.apply(g, alpha, beta))
  }

  @throws[IOException]
  def create(): Unit = {
    val table: HTableDescriptor = new HTableDescriptor(TableName.valueOf(this.tableName))
    if (this.admin.tableExists(table.getTableName)) {
      this.admin.disableTable(table.getTableName)
      this.admin.deleteTable(table.getTableName)
    }
    table.addFamily(new HColumnDescriptor(DEFAULT_CF))
    this.admin.createTable(table)
  }

  def rangeQuery(lng1: Double, lat1: Double, lng2: Double, lat2: Double, indexMap: scala.collection.Map[Long, List[Int]]): java.util.List[Geometry] = {
    val time = System.currentTimeMillis()
    val ranges = locSIndex.ranges(lng1, lat1, lng2, lat2, indexMap)
    val scan = new Scan
    scan.setCaching(1000)
    val filters: java.util.List[Filter] = new java.util.ArrayList[Filter](2)
    val rowRanges = new java.util.ArrayList[MultiRowRangeFilter.RowRange]
    import scala.collection.JavaConversions._
    for (a <- ranges) {
      val startRow = new Array[Byte](8)
      val endRow = new Array[Byte](8)
      ByteArrays.writeLong(a.lower, startRow, 0)
      ByteArrays.writeLong(a.upper + 1L, endRow, 0)
      //println(a)
      rowRanges.add(new MultiRowRangeFilter.RowRange(startRow, true, endRow, true))
    }
    filters.add(new MultiRowRangeFilter(rowRanges))
    val filterList = new FilterList(filters)
    scan.setFilter(filterList)
    var resultScanner: ResultScanner = null
    println(s"index time: ${System.currentTimeMillis() - time}")
    try resultScanner = hTable.getScanner(scan)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    val geoms = new java.util.ArrayList[Geometry]
    //assert resultScanner != null;
    if (null != resultScanner) {
      for (res <- resultScanner) {
        val geo = Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(GEOM)))
        val importerWKT = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromWkt).asInstanceOf[OperatorImportFromWkt]
        val geom = importerWKT.execute(0, Geometry.Type.MultiPoint, geo, null).asInstanceOf[MultiPoint]
        geoms.add(geom)
      }
      resultScanner.close()
    }
    geoms
  }
}
