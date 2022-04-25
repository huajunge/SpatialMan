package huajunge.github.io.spatialman.index

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
}
