
/*
 * Copyright 2016 Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package fr.inria.zenith.randomWalk

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
  */
object RandomWalkScala {


  def parseVectors(vector: Vector, long: Long): Array[Float] = {


    val tab = vector.toArray
    val tab2 = new Array[Float](257)
    var i = 0
    for (a <- 1 to tab2.length - 1) {
      tab2(a) = tab(a - 1).toFloat
    }
    tab2(0) = long.toInt

    tab2
  }


  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: randomWalkTimeSeriesGenerator <Output> <TimeSeriesNbr> <TimeSeriesSize>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("randomWalkTimeSeriesGenerator")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(Array[Class[_]](classOf[Array[Float]], classOf[Array[Double]]))

    val sc = new SparkContext(conf)

    val rdd: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, args(1).toLong, args(2).toInt)


    val finalRdd = rdd.zipWithUniqueId().map(x => parseVectors(x._1, x._2))

    finalRdd.saveAsObjectFile(args(0))

    System.out.println(finalRdd.toDebugString)

    sc.stop()


  }

}
