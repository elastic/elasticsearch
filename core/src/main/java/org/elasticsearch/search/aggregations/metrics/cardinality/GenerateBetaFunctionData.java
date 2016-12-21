/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.cardinality;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.util.BigArrays;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GenerateBetaFunctionData {
    private static final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

    public static void main(String[] args) throws Exception {
        Random r = new Random();
        boolean compareWithCalculated = true;
        int precision = 14;
        int numTestRuns = compareWithCalculated ? 1 : 100;
        // int maxCardinality = 10000000;
        // int initialCardinality = 100;
        // int initialStep = 10;
        int maxCardinality = 170000;
        int initialCardinality = 1000;
        int initialStep = 1000;
        File outFile = new File("/Users/colings86/dev/work/git/elasticsearch/gnuplot/coeffData.dat");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))) {
            for (int testRun = 0; testRun < numTestRuns; testRun++) {
                try (HyperLogLogBeta hllb = new HyperLogLogBeta(precision, BigArrays.NON_RECYCLING_INSTANCE, 1)) {

                    int next = initialCardinality;
                    int step = initialStep;
                    Set<Long> distinct = new HashSet<>();

                    for (int i = 1; i <= maxCardinality; ++i) {
                        long h = BitMixer.mix64(r.nextLong());
                        // long val = r.nextLong();
                        // byte[] valBytes = String.valueOf(val).getBytes();
                        // long h = MurmurHash3.hash128(valBytes, 0,
                        // valBytes.length, 0, hash).h1;
                        hllb.collect(0, h);
                        distinct.add(h);

                        if (i == next) {
                            StringBuilder sb = new StringBuilder();
                            sb.append(distinct.size());
                            sb.append(" ");
                            int z = hllb.getZ(0);
                            sb.append(z);
                            sb.append(" ");

                            double idealBeta = hllb.calculateIdealBeta(0, distinct.size());
                            sb.append(idealBeta);
                            if (compareWithCalculated) {
                                sb.append(" ");
                                double calculatedBeta = hllb.calculateBeta(14, z);
                                sb.append(calculatedBeta);
                                sb.append(" ");
                                double relativeError = (calculatedBeta - idealBeta) / idealBeta * 100.0;
                                sb.append(relativeError);
                            }
                            sb.append('\n');

                            if (compareWithCalculated) {
                                System.out.print(sb.toString());
                            } else {
                                writer.write(sb.toString());
                            }
                            next += step;
                            // if (next >= 100 * step) {
                            // step *= 10;
                            // }
                        }
                    }
                }
                System.out.println((testRun + 1) + " test runs complete");
            }
        }
    }
}
