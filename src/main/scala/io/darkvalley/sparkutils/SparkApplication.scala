/*
 * The MIT License (MIT)
 * Copyright (c) 2016 Antonio Murgia and associates, represented under the name of DarkValley
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

/**
  * SparkAdditions
  *
  * @author Murgia Antonio
  * @version 19/06/16
  */
trait SparkApplication[T,K] {

  /**
    * @param args the usual Array[String] of args
    * @return a typed representation of arguments.
    */
  def parseArguments(args: Array[String]): T

  /**
    * Initializes the Spark application it does takes in input the parsed args and
    * returns back the parsed args and some additional info.
    */
  def init(parsedArgs: T): (T,K)

  /**
    * Business logic of the Spark application, takes in input parsed args and init info
    * and actually performs the logic of your application.
    */
  def run(parsedArgs: T, initInfo: K): Int

  /**
    * Hook in order to carry out some additional operations after your business logic ends.
    * It will be executed either if the application is executed correctly either if the execution is
    * interrupted by an uncaught exception.
    */
  def shutdown(parsedArgs: T, initInfo: K): Int

  /**
    * Hook on uncaught exceptions.
    */
  val onError: PartialFunction[(Throwable, T, K), Int]

  def main(args: Array[String]): Unit = {
    val parsedArgs = parseArguments(args)
    val (modifiedArgs, initInfo) = init(parsedArgs)
    val result = try {
      run(modifiedArgs, initInfo)
    }
    catch{
      case e: Throwable => onError(e, parsedArgs, initInfo)
    }
    finally {
      shutdown(parsedArgs, initInfo)
    }
  }
}
