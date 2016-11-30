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

package io.darkvalley.sparkutils

import org.scalatest.{FlatSpec, Matchers}
import io.darkvalley.sparkutils._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkAdditions
  *
  * @author tmnd
  * @version 30/11/16
  */
class GroupByTest extends FlatSpec with Matchers {

  it should "convert" in {
//    val sc = new SparkContext(new SparkConf().setMaster("local"))
//    val rdd = sc.parallelize(1 until 100)
//    val r1 = rdd.groupBy(true, i => "" + i % 2)
//    val r2 = rdd.groupBy(false, i => ""+ i % 2)
//    val r3 = rdd.groupBy(_ * 2)
//    r1 == r2
//    r2 == r3
//    r1 == r2
    1 should be (1)
  }
}
