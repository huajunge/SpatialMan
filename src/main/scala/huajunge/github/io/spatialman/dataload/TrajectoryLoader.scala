package huajunge.github.io.spatialman.dataload

import com.esri.core.geometry.MultiPoint
import huajunge.github.io.spatialman.index.LocSIndex
import org.apache.spark.{SparkConf, SparkContext}

object TrajectoryLoader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val context = new SparkContext(conf)
    //val file = args(0)
    val file = "D:\\工作文档\\data\\T-drive\\release\\tdrive\\tmp"
    val traj = context.textFile(file, 100)
    import com.esri.core.geometry.{Operator, OperatorFactoryLocal, OperatorImportFromWkt}
    traj.zipWithIndex().map(tra => {
      val t = tra._1.split("-")
      val tid = tra._2
      val index = LocSIndex.apply(15, 5, 5)
      //val geo = WKTUtils.read(t(1)).asInstanceOf[MultiPoint]
      //index.index(geo)
      //index.index2(geo)
      import com.esri.core.geometry.Geometry
      val importerWKT = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromWkt).asInstanceOf[OperatorImportFromWkt]
      val geo = importerWKT.execute(0, Geometry.Type.MultiPoint, t(1), null).asInstanceOf[MultiPoint]
      println(s"${index.index(geo)._1},${index.index(geo)._2}, ${index.index(geo)._2.toBinaryString}")
    }).count()
  }
}
