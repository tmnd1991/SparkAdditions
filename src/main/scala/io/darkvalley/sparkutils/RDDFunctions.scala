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

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import it.unimi.dsi.fastutil.objects.{ObjectArrayList}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
  * SparkAdditions
  *
  * @author tmnd
  * @version 30/11/16
  */
class RDDFunctions[T](self: RDD[T])(implicit tt: ClassTag[T]) {

  def groupBy[K](mapSideCombine: Boolean, partitioner: Partitioner, func: T => K)
                (implicit ct: ClassTag[K]): RDD[(K, Iterable[T])] = {
    val createCombiner: (T) => ArrayBuffer[T] = (v: T) => ArrayBuffer(v)
    val mergeValue = (buf: ArrayBuffer[T], v: T) => buf += v
    val mergeCombiners = (c1: ArrayBuffer[T], c2: ArrayBuffer[T]) => c1 ++= c2
    self.keyBy(func).combineByKeyWithClassTag[ArrayBuffer[T]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine).asInstanceOf[RDD[(K, Iterable[T])]]
  }

  def groupBy[K](mapSideCombine: Boolean, func: T => K)(implicit ct: ClassTag[K]): RDD[(K, Iterable[T])] = {
    groupBy(mapSideCombine, Partitioner.defaultPartitioner(self), func)
  }

  def groupByFast[K](mapSideCombine: Boolean, partitioner: Partitioner, func: T => K)
                    (implicit ct: ClassTag[K]): RDD[(K, Iterable[T])] = {
    val createCombiner = (v: T) => {
      val list = new ObjectArrayList[T]()
      list.add(v)
      list
    }
    val mergeValue = (buf: ObjectArrayList[T], v: T) => {
      buf.add(v)
      buf
    }
    val mergeCombiners = (c1: ObjectArrayList[T], c2: ObjectArrayList[T]) => {
      c1.addAll(c2)
      c1
    }
    self.keyBy(func).combineByKeyWithClassTag[ObjectArrayList[T]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine).map {
      case (key, values) => (key, values.asScala.toIterable)
    }
  }

  def groupByFast[K](mapSideCombine: Boolean, func: T => K): RDD[(K, Iterable[T])] = {
    groupByFast(mapSideCombine, Partitioner.defaultPartitioner(self), func)
  }
}
