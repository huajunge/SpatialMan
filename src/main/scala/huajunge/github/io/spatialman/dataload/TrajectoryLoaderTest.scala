package huajunge.github.io.spatialman.dataload

import com.esri.core.geometry._
import huajunge.github.io.spatialman.client.Constants.DEFAULT_CF
import huajunge.github.io.spatialman.index.LocSIndex
import huajunge.github.io.spatialman.utils.{HClient, PutUtils}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object TrajectoryLoaderTest {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val filePath = args(0)
    val tableName = args(1)
    val g = args(2).toShort
    val alpha = args(3).toInt
    val beta = args(4).toInt
    val table = new HTableDescriptor(TableName.valueOf(tableName))
    if (admin.tableExists(table.getTableName)) {
      admin.disableTable(table.getTableName)
      admin.deleteTable(table.getTableName)
    }
    if (!admin.tableExists(table.getTableName)) {
      val table = new HTableDescriptor(TableName.valueOf(tableName))
      table.addFamily(new HColumnDescriptor(DEFAULT_CF))
      admin.createTable(table)
    }
    val job = new JobConf(hbaseConf)
    job.setOutputFormat(classOf[TableOutputFormat])
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val context = new SparkContext(conf)
    val bounds = new Envelope(115.7542, 39.289, 117.514, 40.794)

    //val file = args(0)
    //val file = "D:\\工作文档\\data\\T-drive\\release\\tdrive\\tmp"
    val traj = context.textFile(filePath, 2)
    val index = new LocSIndex(g, (115.7542, 117.514), (39.289, 40.794), alpha, beta)
    import com.esri.core.geometry.{Operator, OperatorFactoryLocal, OperatorImportFromWkt}
    val indexedTraj = traj.map(tra => {
      val t = tra.split("-")
      val tid = t(0)
      //val geo = WKTUtils.read(t(1)).asInstanceOf[MultiPoint]
      //index.index(geo)
      //index.index2(geo)
      import com.esri.core.geometry.Geometry
      val index = new LocSIndex(g, (115.7542, 117.514), (39.289, 40.794), alpha, beta)
      val importerWKT = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromWkt).asInstanceOf[OperatorImportFromWkt]
      val geo = importerWKT.execute(0, Geometry.Type.MultiPoint, t(1), null).asInstanceOf[MultiPoint]
      if (GeometryEngine.contains(bounds, geo, SpatialReference.create(4326))) {
        val indexValue = index.index(geo)
        val put = PutUtils.getPut(tid, t(1), indexValue._2, indexValue._3)
        println(s"${indexValue._1},${indexValue._2}, ${indexValue._2.toBinaryString}")
        println(s"${indexValue._2 | (indexValue._3.toLong << 32)}")
        (indexValue, put)
      } else {
        null
      }
    }).filter(v => v != null)
    indexedTraj.map(v => (new ImmutableBytesWritable(), v._2)).saveAsHadoopDataset(job)
    val indexMap = indexedTraj.map(v => (v._1._2, v._1._3)).distinct().groupByKey().collectAsMap().mapValues(v => v.toList)
    //Query
    context.stop()
    val client = new HClient(tableName, g, alpha, beta, index)
    //val queryRange = (116.51386, 39.99955, 116.52386, 40.00055)
    val queryRange = Array(116.71717702334188, 39.66302489185778, 116.80000369674876, 39.70368525880298)
    val result = client.rangeQuery(queryRange(0),queryRange(1),queryRange(2),queryRange(3), indexMap)
    val time2 = System.currentTimeMillis()
    client.rangeQuery(queryRange(0),queryRange(1),queryRange(2),queryRange(3), indexMap)
    client.rangeQuery(queryRange(0),queryRange(1),queryRange(2),queryRange(3), indexMap)
    println(System.currentTimeMillis() - time2)
    println(result.size())
    val time = System.currentTimeMillis()
    client.rangeQuery(queryRange(0),queryRange(1),queryRange(2),queryRange(3), indexMap)
    println(System.currentTimeMillis() - time)
    println(result.size())
    //val env = new Envelope(minX, minY, minX + w, minY + h)
    val env = new Envelope(queryRange(0),queryRange(1),queryRange(2),queryRange(3))
    var count = 0
    println(GeometryEngine.geometryToWkt(env, 0))
    for (elem <- result.asScala) {
      //println(GeometryEngine.geometryToWkt(elem,0))
      if (OperatorIntersects.local().execute(env, elem, SpatialReference.create(4326), null)) {
        count += 1
      }
    }
    println(count)
  }
}
