package huajunge.github.io.spatialman.index

import java.util

class XZSFC(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) {
  val xLo = xBounds._1
  val xHi = xBounds._2
  val yLo = yBounds._1
  val yHi = yBounds._2

  val xSize = xHi - xLo
  val ySize = yHi - yLo

  def normalize(xmin: Double,
                ymin: Double,
                xmax: Double,
                ymax: Double,
                lenient: Boolean): (Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax, s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax]")

    try {
      require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi,
        s"Values out of bounds ([$xLo $xHi] [$yLo $yHi]): [$xmin $xmax] [$ymin $ymax]")

      val nxmin = (xmin - xLo) / xSize
      val nymin = (ymin - yLo) / ySize
      val nxmax = (xmax - xLo) / xSize
      val nymax = (ymax - yLo) / ySize

      (nxmin, nymin, nxmax, nymax)
    } catch {
      case _: IllegalArgumentException if lenient =>

        val bxmin = if (xmin < xLo) {
          xLo
        } else if (xmin > xHi) {
          xHi
        } else {
          xmin
        }
        val bymin = if (ymin < yLo) {
          yLo
        } else if (ymin > yHi) {
          yHi
        } else {
          ymin
        }
        val bxmax = if (xmax < xLo) {
          xLo
        } else if (xmax > xHi) {
          xHi
        } else {
          xmax
        }
        val bymax = if (ymax < yLo) {
          yLo
        } else if (ymax > yHi) {
          yHi
        } else {
          ymax
        }

        val nxmin = (bxmin - xLo) / xSize
        val nymin = (bymin - yLo) / ySize
        val nxmax = (bxmax - xLo) / xSize
        val nymax = (bymax - yLo) / ySize

        (nxmin, nymin, nxmax, nymax)
    }
  }

  case class QueryWindow(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {
    def insertion(x1: Double, y1: Double, x2: Double, y2: Double): Boolean = {
      xmax >= x1 && ymax >= y1 && xmin <= x2 && ymin <= y2
    }
  }

  case class EE(xmin: Double, ymin: Double, xmax: Double, ymax: Double, level: Int, elementCode: Long) {
    val xLength = xmax - xmin
    val yLength = ymax - ymin
    val exMax = xmin + xLength * alpha
    val eyMax = ymin + yLength * beta
    val children = new java.util.ArrayList[EE](4)

    var shapes: List[Int] = null

    def insertion(window: QueryWindow): Boolean = {
      window.xmax >= xmin && window.ymax >= ymin && window.xmin <= exMax && window.ymin <= eyMax
    }

    def isContained(window: QueryWindow): Boolean = {
      window.xmin <= xmin && window.ymin <= ymin && window.xmax >= exMax && window.ymax >= eyMax
    }

    def insertSignature(window: QueryWindow): Int = {
      var signature = 0
      for (i <- 0 until alpha) {
        for (j <- 0 until beta) {
          val minX = xmin + xLength * i
          val minY = ymin + yLength * j
          //env.in
          if (window.insertion(minX, minY, minX + xLength, ymin + yLength)) {
            signature |= (1 << (i * beta + j))
          }
        }
      }
      signature
    }

    override def toString: String = {
      s"POLYGON (($xmin $ymin,$xmin $eyMax,$exMax $eyMax,$exMax $ymin,$xmin $ymin))"
    }

    def split(): Unit = {
      if (children.isEmpty) {
        val xCenter = (xmax + xmin) / 2.0
        val yCenter = (ymax + ymin) / 2.0
        children.add(EE(xmin, ymin, xCenter, yCenter, level + 1, elementCode + 1L))
        children.add(EE(xCenter, ymin, xmax, yCenter, level + 1, elementCode + 1L + +1L * (math.pow(4, maxR - level).toLong - 1L) / 3L))
        children.add(EE(xmin, yCenter, xCenter, ymax, level + 1, elementCode + 1L + +2L * (math.pow(4, maxR - level).toLong - 1L) / 3L))
        children.add(EE(xCenter, yCenter, xmax, ymax, level + 1, elementCode + 1L + +3L * (math.pow(4, maxR - level).toLong - 1L) / 3L))
      }
    }

    def getShapes(indexMap: scala.collection.Map[Long, List[Int]]): List[Int] = {
      if (null != shapes) {
        return shapes
      }
      val indexSpaces = indexMap.get(elementCode)
      if (indexSpaces.isDefined) {
        shapes = indexSpaces.get
      }
      shapes
    }

    def getChildren: util.ArrayList[EE] = {
      if (children.isEmpty) {
        split()
      }
      children
    }
  }
}
