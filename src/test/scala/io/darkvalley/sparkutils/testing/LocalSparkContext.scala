/*
 * The MIT License (MIT)
 * Copyright (c) ${YEAR} Antonio Murgia and associates, represented under the name of DarkValley
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 *  associated documentation files (the "Software"), to deal in the Software without restriction,
 *  including without limitation the rights to use, copy, modify, merge, publish, distribute,
 *  sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 *  substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.darkvalley.sparkutils.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * This is a commodity trait to be mixed to scalatest suites in order to have a freshly instantiated
  * SparkContext during the test run accessible using the [sparkContext] method.
  *
  * @author Antonio Murgia
  * @version 25/03/16
  */
trait LocalSparkContext extends Suite with BeforeAndAfterAll {
  this: Suite =>

  implicit lazy val sc = sparkContext
  protected val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName(getClass.getSimpleName)
    .set("spark.ui.enabled", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.validateOutputSpecs", "false")
  private var _sc: SparkContext = _

  def sparkContext: SparkContext = _sc

  def numThreads: Option[Int] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    System.clearProperty("spark.driver.property")
    System.clearProperty("spark.hostPort")
    _sc = numThreads match {
      case Some(t) => new SparkContext(sparkConf.setMaster(s"local[$t]"))
      case None => new SparkContext(sparkConf)
    }
  }

  override def afterAll(): Unit = {
    _sc.stop()
    _sc = null
    System.clearProperty("spark.driver.property")
    System.clearProperty("spark.hostPort")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    super.afterAll()
  }


  def timing[T](f: => T): (T, Long) = {
    val start = System.nanoTime()
    val r = f
    (r, System.nanoTime() - start)
  }
}
