package huajunge.github.io.spatialman.index

import com.esri.core.geometry._
import org.locationtech.sfcurve.IndexRange

import scala.collection.JavaConverters._


class LocSIndex(maxR: Short, xBounds: (Double, Double), yBounds: (Double, Double), alpha: Int, beta: Int) extends XZSFC(maxR, xBounds, yBounds, alpha, beta) with Serializable {


  def signature(x: Double, y: Double, w: Double, h: Double, geometry: Geometry): Int = {
    var signature = 0
    //    for (i <- 0 until ((alpha * beta) / 8 + 1)) {
    //      signature(i) = 0
    //    }
    for (i <- 0 until alpha) {
      for (j <- 0 until beta) {
        val minX = x + w * i
        val minY = y + h * j
        val env = new Envelope(minX, minY, minX + w, minY + h)
        //env.in
        if (OperatorIntersects.local().execute(env, geometry, SpatialReference.create(4326), null)) {
          signature |= (1 << (i * beta + j))
        }
      }
    }
    signature
  }

  def sequenceCode(x: Double, y: Double, l: Int): Long = {
    var i = 1
    var xmin = xLo
    var ymin = yLo
    var xmax = xHi
    var ymax = yHi
    var cs = 0L
    while (i <= l) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      (x < xCenter, y < yCenter) match {
        case (true, true) => cs += 1L; xmax = xCenter; ymax = yCenter
        case (false, true) => cs += 1L + 1L * IS(i); xmin = xCenter; ymax = yCenter
        case (true, false) => cs += 1L + 2L * IS(i); xmax = xCenter; ymin = yCenter
        case (false, false) => cs += 1L + 3L * IS(i); xmin = xCenter; ymin = yCenter
      }
      i += 1
    }
    cs
  }

  def IS(i: Int): Long = {
    (math.pow(4, maxR - i + 1).toLong - 1L) / 3L
  }

  def index(geometry: Geometry, lenient: Boolean = false): (Int, Long, Int) = {
    val mbr = new Envelope2D()
    geometry.queryEnvelope2D(mbr)
    var maxDim = math.min(xSize * alpha.toDouble / (mbr.xmax - mbr.xmin), ySize * beta.toDouble / (mbr.ymax - mbr.ymin))
    var l = math.floor(math.log(maxDim) / math.log(2)).toInt
    if (l >= maxR) {
      l = maxR
    } else {
      val w = xSize * math.pow(0.5, l)
      val h = ySize * math.pow(0.5, l)

      def predicateX(min: Double, max: Double): Boolean = max <= (math.floor(min / w) * w) + (alpha * w)

      def predicateY(min: Double, max: Double): Boolean = max <= (math.floor(min / h) * h) + (beta * h)

      if (!predicateX(mbr.xmin, mbr.xmax) || !predicateY(mbr.ymin, mbr.ymax)) {
        l = l - 1
        if (l < 0) l = 0
      }
    }
    val w = xSize * math.pow(0.5, l)
    val h = ySize * math.pow(0.5, l)
    val x = math.floor(mbr.xmin / w) * w
    val y = math.floor(mbr.ymin / h) * h
    val sig = signature(x, y, w, h, geometry)
    val localtion = sequenceCode(mbr.xmin, mbr.ymin, l)
    (l, localtion, sig)
  }

  //  def ranges(lat1: Double, lng1: Double, lat2: Double, lng2: Double): java.util.List[IndexRange] = {
  //
  //  }

  def ranges(lng1: Double, lat1: Double, lng2: Double, lat2: Double, indexMap: scala.collection.Map[Long, List[Int]]): java.util.List[IndexRange] = {
    val queryWindow = QueryWindow(lng1, lat1, lng2, lat2)
    val ranges = new java.util.ArrayList[IndexRange](100)
    val remaining = new java.util.ArrayDeque[EE](200)
    val levelStop = EE(-1, -1, -1, -1, -1, -1)
    val root = EE(xBounds._1, yBounds._1, xBounds._2, yBounds._2, 0, 0L)
    root.split()
    root.children.asScala.foreach(remaining.add)
    remaining.add(levelStop)
    var level: Short = 1
    //val executor = Executors.newFixedThreadPool(8)
    while (!remaining.isEmpty) {
      val next = remaining.poll
      if (next.eq(levelStop)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty && level < maxR) {
          level = (level + 1).toShort
          remaining.add(levelStop)
        }
      } else {
        checkValue(next, level)
      }
    }

    def checkValue(quad: EE, level: Short): Unit = {
      if (quad.isContained(queryWindow)) {
        val (min, max) = (quad.elementCode, quad.elementCode + IS(level) - 1L)
        println(quad.toString)
        ranges.add(IndexRange(min, max, contained = true))
      } else if (quad.insertion(queryWindow)) {
        val key = quad.elementCode
        val signature = quad.insertSignature(queryWindow)
        val indexSpaces = indexMap.get(key)
        if (indexSpaces.isDefined) {
          println(quad.toString)
          for (elem <- indexSpaces.get) {
            if ((signature | elem) >= 0) {
              val min = key | (elem.toLong << 32)
              val range = IndexRange(min, min, contained = false)
              ranges.add(range)
            }
          }
        }
        //executor.execute(new SignatureTask(quad.elementCode, quad.insertSignature(queryWindow)))
        if (level < maxR) {
          quad.split()
          quad.children.asScala.foreach(remaining.add)
        }
      }
    }

    class SignatureTask(val key: Long, val signature: Int) extends Runnable {
      override def run(): Unit = {
        val indexSpaces = indexMap.get(key.toInt)
        if (indexSpaces.isDefined) {
          for (elem <- indexSpaces.get) {
            if ((signature | elem) >= 0) {
              val min = key | (elem.toLong << 32)
              val range = IndexRange(min, min, contained = false)
              ranges.add(range)
            }
          }
        }
      }
    }
    //executor.execute()
    //    executor.shutdown()
    //    executor.awaitTermination(10, TimeUnit.SECONDS)
    //    if (ranges.size() > 0) {
    //      ranges.sort(IndexRange.IndexRangeIsOrdered)
    //      var current = ranges.get(0) // note: should always be at least one range
    //      val result = ArrayBuffer.empty[IndexRange]
    //      var i = 1
    //      while (i < ranges.size()) {
    //        val range = ranges.get(i)
    //        if (range.lower <= current.upper + 1) {
    //          current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
    //        } else {
    //          result.append(current)
    //          current = range
    //        }
    //        i += 1
    //      }
    //      result.append(current)
    //      result.asJava
    //    } else {
    //      ranges
    //    }
    ranges
  }

  //  def index(geometry: Geometry, lenient: Boolean = false): (Int, Double, Double) = {
  //    val mbr = new Envelope2D()
  //    geometry.queryEnvelope2D(mbr)
  //    val (nxmin, nymin, nxmax, nymax) = normalize(mbr.xmin, mbr.ymin, mbr.xmax, mbr.ymax, lenient)
  //    val maxDim = math.max((nxmax - nxmin) / alpha.toDouble, (nymax - nymin) / beta.toDouble)
  //    var l = math.floor(math.log(maxDim) / math.log(0.5)).toInt
  //    if (l >= maxR) {
  //      l = maxR
  //    } else {
  //      val w = math.pow(0.5, l)
  //      val h = math.pow(0.5, l)
  //
  //      def predicateX(min: Double, max: Double): Boolean = max <= (math.floor(min / w) * w) + (alpha * w)
  //
  //      def predicateY(min: Double, max: Double): Boolean = max <= (math.floor(min / h) * h) + (beta * h)
  //
  //      if (!predicateX(nxmin, nxmax) || !predicateY(nymin, nymax)) {
  //        l = l - 1
  //        if (l < 0) l = 0
  //      }
  //    }
  //    val w = math.pow(0.5, l)
  //    val x = math.floor(nxmin / w) * w
  //    val y = math.floor(nymin / w) * w
  //    //val sig = signature(x, y, w, geometry)
  //    (l, x * xSize + xLo, y * ySize + yLo)
  //  }
}

object LocSIndex extends Serializable {
  // the initial level of quads
  private val cache = new java.util.concurrent.ConcurrentHashMap[(Short, Int, Int), LocSIndex]()

  def apply(g: Short, alpha: Int, beta: Int): LocSIndex = {
    var sfc = cache.get((g, alpha, beta))
    if (sfc == null) {
      sfc = new LocSIndex(g, (-180.0, 180.0), (-90.0, 90.0), alpha, beta)
      cache.put((g, alpha, beta), sfc)
    }
    sfc
  }
}