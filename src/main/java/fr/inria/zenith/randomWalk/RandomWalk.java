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
package fr.inria.zenith.randomWalk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import static org.apache.spark.mllib.random.RandomRDDs.normalJavaVectorRDD;
import scala.Tuple2;

/**
 *
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class RandomWalk {

    public static void main(String[] args) {

        if(args.length < 2) {
            System.err.println( "Usage: randomWalkTimeSeriesGenerator <Output> <TimeSeriesNbr> <TimeSeriesSize>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("randomWalkTimeSeriesGenerator")
                                        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                        .set("spark.kryo.registrationRequired", "true");

        conf.registerKryoClasses(new Class<?>[] {
            float[].class,
            double[].class
        });

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Vector> rdd = normalJavaVectorRDD(jsc, Long.parseLong(args[1]), Integer.parseInt(args[2]));

        JavaRDD<float[]> finalRdd = rdd.zipWithUniqueId().map(new Function<Tuple2<Vector, Long>, float[]>() {

            @Override
            public float[] call(Tuple2<Vector, Long> t1) throws Exception {
                double[] tstemp = t1._1.toArray();
                float[] ts = new float[tstemp.length];
                for(int i = 1; i < ts.length; i++) {
                    ts[i] = ts[i - 1] + (float) tstemp[i - 1];
                }
                ts[0] = t1._2;
                return ts;
            }
        });
        finalRdd.saveAsObjectFile(args[0]);
        System.out.println(finalRdd.toDebugString());

        jsc.stop();

    }
}
