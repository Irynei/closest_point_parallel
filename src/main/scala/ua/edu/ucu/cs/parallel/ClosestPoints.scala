package ua.edu.ucu.cs.parallel
import org.scalameter

import scala.util.Random
import scala.math.{pow, sqrt}
import org.scalameter._

/**
  * Created by irynei on 11.05.18.
  */

object ClosestPoints {

  def distance2D(p1: (Double, Double), p2: (Double, Double)): Double =
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))

  def closestPairSeq(inputPoints: List[(Double, Double)]): (List[(Double, Double)], Double) = {
//    brute force realization
    var dist = Double.MaxValue
    var pair = List[(Double, Double)]()
    for (i <- 0 until inputPoints.length - 1) {
      for (j <- i + 1 until inputPoints.length) {
        val newDist = distance2D(inputPoints(i), inputPoints(j))
        if (newDist < dist) {
          dist = newDist
          pair = List(inputPoints(i), inputPoints(j))
        }
      }
    }
    (pair, dist)
  }

  def splitInHalf(list: List[(Double, Double)]): (Double, List[(Double, Double)], List[(Double, Double)]) = {
    val (left, right) = list.sortWith(_._1 < _._1).splitAt(list.size / 2)
    //    get median elem and left and right parts
    (right.head._1, left, right)
  }

  def boundaryMerge(closestPair: List[(Double, Double)], minDist: Double, points: List[(Double, Double)],
                    medianX: Double): (List[(Double, Double)], Double) = {
//    get points indide boundary and sort by `y` coordinate
    val pointsInside = points.filter(p => p._1 <= medianX + minDist && p._1 >= medianX - minDist).sortWith(_._2 < _._2)
    val size = pointsInside.length
    var (pair, dist) = (closestPair, minDist)
//    check for min dist using at most 7 points for each point
    for (i <- 0 until size - 1) {
      for (j <- i + 1 until Math.min(i + 8, size)) {
        val newDist = distance2D(pointsInside(i), pointsInside(j))
        if (newDist < dist) {
          dist = newDist
          pair = List(pointsInside(i), pointsInside(j))
        }
      }
    }
    (pair, dist)
  }

  def closestPairPar(inputPoints: List[(Double, Double)], sizePerCore: Int): (List[(Double, Double)], Double) = {
//    if size <= 2 return dist
    if (inputPoints.size <= 2) {
      val dist = if (inputPoints.head == inputPoints.last) Double.MaxValue else distance2D(inputPoints.head, inputPoints.last)
      (inputPoints, dist)
    }
    else {
//      split points in half
      val (midX, leftPart, rightPart) = splitInHalf(inputPoints)

//      decide either to make recursive call in parallel or sequentially
      val ((leftClosestPoints, leftMinDistance), (rightClosestPoints, rightMinDistance)) =
        if (leftPart.length >= sizePerCore && rightPart.length >= sizePerCore)
          parallel(closestPairPar(leftPart, sizePerCore), closestPairPar(rightPart, sizePerCore))
        else
          (closestPairPar(leftPart, sizePerCore), closestPairPar(rightPart, sizePerCore))

//      boundary merge
      if (leftMinDistance < rightMinDistance)
        boundaryMerge(leftClosestPoints, leftMinDistance, inputPoints, midX)
      else
        boundaryMerge(rightClosestPoints, rightMinDistance, inputPoints, midX)
    }
  }


  def main(args: Array[String]) {
    val rnd = new Random
    val maxCores = 4
    val inputSize = 1000
    val sizePerCore = inputSize / maxCores

    println(s"Cores: $maxCores, input size: $inputSize,  size per core: $sizePerCore")
    val inputList = (0 until inputSize).map(_ => (rnd.nextDouble(), rnd.nextDouble())).toList.sortWith(_._1 < _._1)


    println(s"Parallel: ${closestPairPar(inputList, sizePerCore)}")
    println(s"Sequential: ${closestPairSeq(inputList)}")

//    compare performance using Scala meter
    val standartConfig = config(
      Key.exec.minWarmupRuns -> 20,
      Key.exec.maxWarmupRuns -> 50,
      Key.exec.benchRuns -> 40,
      Key.verbose -> true
    ) withWarmer new scalameter.Warmer.Default


    val seqtime = standartConfig.measure {
      closestPairPar(inputList, inputList.length)
    }

    val partime = standartConfig.measure {
      closestPairPar(inputList, sizePerCore)
    }

    println(s"sequential time $seqtime ms")
    println(s"parallel time $partime ms")
    println(s"speedup: ${seqtime.value / partime.value}")
  }
}
