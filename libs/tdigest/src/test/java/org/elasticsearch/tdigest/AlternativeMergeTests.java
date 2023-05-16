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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class AlternativeMergeTests extends ESTestCase {
    /**
     * Computes size using the alternative scaling limit for both an idealized merge and for
     * a MergingDigest.
     *
     * This test does some sanity checking, but the real purpose is to create data files
     * <code>sizes.csv</code> and <code>counts.csv</code>
     * @throws FileNotFoundException If output files can't be created.
     */
    public void testMerges() throws IOException {
        try (
            PrintWriter sizes = new PrintWriter("sizes.csv", StandardCharsets.UTF_8);
            PrintWriter out = new PrintWriter("counts.csv", StandardCharsets.UTF_8)
        ) {
            sizes.printf(Locale.ROOT, "algo, counts, digest, compression, n\n");
            out.printf(Locale.ROOT, "algo, compression, n, q, count\n");
            for (int n : new int[] { 100, 1000, 10000, 100000 }) {
                for (double compression : new double[] { 50, 100, 200, 400 }) {
                    MergingDigest digest1 = new MergingDigest(compression);
                    AVLTreeDigest digest2 = new AVLTreeDigest(compression);
                    List<Double> data = new ArrayList<>();
                    Random gen = random();
                    for (int i = 0; i < n; i++) {
                        double x = gen.nextDouble();
                        data.add(x);
                        digest1.add(x);
                        digest2.add(x);
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
                    sizes.printf(Locale.ROOT, "%s, %d, %d, %.0f, %d\n", "merge", counts.size(), digest1.centroids().size(), compression, n);
                    sizes.printf(Locale.ROOT, "%s, %d, %d, %.0f, %d\n", "tree", counts.size(), digest2.centroids().size(), compression, n);
                    sizes.printf(Locale.ROOT, "%s, %d, %d, %.0f, %d\n", "ideal", counts.size(), counts.size(), compression, n);
                    soFar = 0;
                    for (Double count : counts) {
                        out.printf(Locale.ROOT, "%s, %.0f, %d, %.3f, %.0f\n", "ideal", compression, n, (soFar + count / 2) / n, count);
                        soFar += count;
                    }
                    assertEquals(n, soFar, 0);
                    soFar = 0;
                    for (Centroid c : digest1.centroids()) {
                        out.printf(
                            Locale.ROOT,
                            "%s, %.0f, %d, %.3f, %d\n",
                            "merge",
                            compression,
                            n,
                            (soFar + c.count() / 2) / n,
                            c.count()
                        );
                        soFar += c.count();
                    }
                    assertEquals(n, soFar, 0);
                    soFar = 0;
                    for (Centroid c : digest2.centroids()) {
                        out.printf(Locale.ROOT, "%s, %.0f, %d, %.3f, %d\n", "tree", compression, n, (soFar + c.count() / 2) / n, c.count());
                        soFar += c.count();
                    }
                    assertEquals(n, soFar, 0);
                }
            }
        }
    }
}
