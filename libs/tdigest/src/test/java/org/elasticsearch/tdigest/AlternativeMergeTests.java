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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class AlternativeMergeTests extends TDigestTestCase {
    /**
     * Computes size using the alternative scaling limit for both an idealized merge and for
     * a MergingDigest.
     */
    public void testMerges() {
        for (int n : new int[] { 100, 1000, 10000, 100000 }) {
            for (double compression : new double[] { 50, 100, 200, 400 }) {
                try (
                    MergingDigest mergingDigest = TDigest.createMergingDigest(arrays(), compression);
                    AVLTreeDigest treeDigest = TDigest.createAvlTreeDigest(arrays(), compression);
                ) {
                    List<Double> data = new ArrayList<>();
                    Random gen = random();
                    for (int i = 0; i < n; i++) {
                        double x = gen.nextDouble();
                        data.add(x);
                        mergingDigest.add(x);
                        treeDigest.add(x);
                    }
                    Collections.sort(data);
                    List<Double> counts = new ArrayList<>();
                    double soFar = 0;
                    double current = 0;
                    for (Double x : data) {
                        double q = (soFar + (current + 1.0) / 2) / n;
                        if (current == 0 || current + 1 < n * Math.PI / compression * Math.sqrt(q * (1 - q))) {
                            current += 1;
                        } else {
                            counts.add(current);
                            soFar += current;
                            current = 1;
                        }
                    }
                    if (current > 0) {
                        counts.add(current);
                    }
                    soFar = 0;
                    for (Double count : counts) {
                        soFar += count;
                    }
                    assertEquals(n, soFar, 0);
                    soFar = 0;
                    for (Centroid c : mergingDigest.centroids()) {
                        soFar += c.count();
                    }
                    assertEquals(n, soFar, 0);
                    soFar = 0;
                    for (Centroid c : treeDigest.centroids()) {
                        soFar += c.count();
                    }
                    assertEquals(n, soFar, 0);
                }
            }
        }
    }
}
