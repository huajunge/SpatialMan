package huajunge.github.io.spatialman.index

import com.esri.core.geometry._


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
          signature |= 1 << ((i * beta + j))
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

      def IS(i: Int): Long = {
        (math.pow(4, maxR - i + 1).toLong - 1L) / 3L
      }
    }
    cs
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