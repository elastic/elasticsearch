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

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MegaMergeTests extends ESTestCase {

    private static final int WIDTH = 1000;
    private static final int DATA_STRIDE = 23;

    public void testLargeMerge() {
        double[][] data = new double[DATA_STRIDE][1500];
        Random gen = random();
        for (int i = 0; i < DATA_STRIDE; i++) {
            for (int j = 0; j < 1500; j++) {
                data[i][j] = gen.nextGaussian();
            }
        }
        final MergingDigest[] td = new MergingDigest[WIDTH];
        for (int i = 0, m = 0; i < WIDTH; i++) {
            td[i] = new MergingDigest(100);
            for (int k = 0; k < 1500; k++) {
                td[i].add(data[m][k]);
            }
            m = (m + 1) % DATA_STRIDE;
        }

        MergingDigest merged = new MergingDigest(100);
        merged.add(Arrays.stream(td).toList());
    }

    public void testMegaMerge() {
        final int SUMMARIES = 100;
        final int POINTS = 100000;
        double[] data = new double[POINTS];
        Random gen = random();
        for (int i = 0; i < data.length; i++) {
            data[i] = gen.nextGaussian();
        }

        // record the basic summaries
        final MergingDigest[] td = new MergingDigest[SUMMARIES];
        int k = 0;
        for (int i = 0; i < SUMMARIES; i++) {
            td[i] = new MergingDigest(200);
            for (int j = 0; j < POINTS; j++) {
                td[i].add(data[k]);
                k = (k + 1) % data.length;
            }
        }

        MergingDigest tAll = new MergingDigest(200);
        tAll.add(List.of(td));
    }
}
