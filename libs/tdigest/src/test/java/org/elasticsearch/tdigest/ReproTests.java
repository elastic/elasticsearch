/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.test.ESTestCase;

import java.util.Random;

public class ReproTests extends ESTestCase {

    public void testRepro() {
        Random gen = random();
        double[] data = new double[10000];
        for (int i = 0; i < data.length; i++) {
            // these samples are truncated and thus have lots of duplicates
            // this can wreak havoc with the t-digest invariants
            data[i] = Math.floor(gen.nextDouble() * 10);
        }

        for (ScaleFunction sf : ScaleFunction.values()) {
            if (sf.toString().contains("NO_NORM")) {
                continue;
            }
            TDigest distLow = new MergingDigest(100);
            TDigest distMedian = new MergingDigest(100);
            TDigest distHigh = new MergingDigest(100);
            for (int i = 0; i < 500; i++) {
                MergingDigest digest = new MergingDigest(100);
                digest.setScaleFunction(ScaleFunction.K_2);
                for (double x : data) {
                    digest.add(x);
                }
                digest.compress();
                distLow.add(digest.quantile(0.001));
                distMedian.add(digest.quantile(0.5));
                distHigh.add(digest.quantile(0.999));
            }
            assertEquals(0, distLow.quantile(0.0), 0);
            assertEquals(0, distLow.quantile(0.5), 0);
            assertEquals(0, distLow.quantile(1.0), 0);
            assertEquals(9, distHigh.quantile(0.0), 0);
            assertEquals(9, distHigh.quantile(0.5), 0);
            assertEquals(9, distHigh.quantile(1.0), 0);
        }
    }
}
