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

import io.darkvalley.sparkutils.groupby.{ArrayBuffer, FastUtil, LinkedList}
import io.darkvalley.sparkutils.testing.LocalSparkContext
import org.scalatest.{FlatSpec, Matchers}

/**
  * SparkAdditions
  *
  * @author tmnd
  * @version 30/11/16
  */
class GroupByTest extends FlatSpec with Matchers with LocalSparkContext {

  override def numThreads: Option[Int] = Some(5)

  it should "compare original and LinkedList implementation" in {
    val rdd = sc.parallelize(1 until 1000, numSlices = 5)
    val keyFunc = (i: Int) => String.valueOf(i)
    val groupBy = rdd.groupBy(keyFunc)
    val customGroupBy = rdd.customGroupBy(mapSideCombine = false, t = LinkedList, keyFunc)
    val customGroupBy2 = rdd.customGroupBy(mapSideCombine = true, t = LinkedList, keyFunc)

    groupBy.collect().toMap should be(customGroupBy.collect().toMap)
    groupBy.collect().toMap should be(customGroupBy2.collect().toMap)
  }

  it should "compare original and ArrayBuffer implementation" in {
    val rdd = sc.parallelize(1 until 1000, numSlices = 5)
    val keyFunc = (i: Int) => String.valueOf(i)
    val groupBy = rdd.groupBy(keyFunc)
    val customGroupBy = rdd.customGroupBy(mapSideCombine = false, t = ArrayBuffer, keyFunc)
    val customGroupBy2 = rdd.customGroupBy(mapSideCombine = true, t = ArrayBuffer, keyFunc)

    groupBy.collect().toMap should be(customGroupBy.collect().toMap)
    groupBy.collect().toMap should be(customGroupBy2.collect().toMap)
  }

  it should "compare original and FastUtil implementation" in {
    val rdd = sc.parallelize(1 until 1000, numSlices = 5)
    val keyFunc = (i: Int) => String.valueOf(i)
    val groupBy = rdd.groupBy(keyFunc)
    val customGroupBy = rdd.customGroupBy(mapSideCombine = false, t = FastUtil, keyFunc)
    val customGroupBy2 = rdd.customGroupBy(mapSideCombine = true, t = FastUtil, keyFunc)

    groupBy.collect().toMap should be(customGroupBy.collect().toMap)
    groupBy.collect().toMap should be(customGroupBy2.collect().toMap)
  }

}
