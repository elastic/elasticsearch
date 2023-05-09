/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.*;

public class MergingDigestTest extends TDigestTest {
    @BeforeClass
    public static void setup() throws IOException {
        TDigestTest.setup("merge");
    }

    protected DigestFactory factory(final double compression) {
        return new DigestFactory() {
            @Override
            public TDigest create() {
                return new MergingDigest(compression);
            }
        };
    }

    @Override
    protected TDigest fromBytes(ByteBuffer bytes) {
        return MergingDigest.fromBytes(bytes);
    }

    // This test came from PR#145 by github user pulver
    @Test
    public void testNanDueToBadInitialization() {
        int compression = 30;
        int factor = 5;
        MergingDigest md = new MergingDigest(compression, (factor + 1) * compression, compression);

        final int M = 10;
        List<MergingDigest> mds = new ArrayList<>();
        for (int i = 0; i < M; ++i) {
            mds.add(new MergingDigest(compression, (factor + 1) * compression, compression));
        }

        // Fill all digests with values (0,10,20,...,80).
        List<Double> raw = new ArrayList<>();
        for (int i = 0; i < 9; ++i) {
            double x = 10 * i;
            md.add(x);
            raw.add(x);
            for (int j = 0; j < M; ++j) {
                mds.get(j).add(x);
                raw.add(x);
            }
        }
        Collections.sort(raw);

        // Merge all mds one at a time into md.
        for (int i = 0; i < M; ++i) {
            List<MergingDigest> singleton = new ArrayList<>();
            singleton.add(mds.get(i));
            md.add(singleton);
        }
//        md.add(mds);

//        Assert.assertFalse(Double.isNaN(md.quantile(0.01)));
        // Output
        System.out.printf("%4s\t%10s\t%10s\t%10s\t%10s\n", "q", "estimated", "actual", "error_cdf", "error_q");
        String dashes = "==========";

        System.out.printf("%4s\t%10s\t%10s\t%10s\t%10s\n", dashes.substring(0, 4), dashes, dashes, dashes, dashes);
        for (double q : new double[]{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99}) {
            double est = md.quantile(q);
            double actual = Dist.quantile(q, raw);
            double qx = md.cdf(actual);
            Assert.assertEquals(q, qx, 0.08);
            Assert.assertEquals(est, actual, 3.5);
            System.out.printf("%4.2f\t%10.2f\t%10.2f\t%10.2f\t%10.2f\n", q, est, actual, Math.abs(est - actual), Math.abs(qx - q));
        }
    }


    /**
     * Verifies interpolation between a singleton and a larger centroid.
     */
    @Test
    public void singleMultiRange() {
        TDigest digest = factory(50).create();
        digest.setScaleFunction(ScaleFunction.K_0);
        for (int i = 0; i < 100; i++) {
            digest.add(1);
            digest.add(2);
            digest.add(3);
        }
        // this check is, of course true, but it also forces merging before we change scale
        assertTrue(digest.centroidCount() < 300);
        digest.add(0);
        // we now have a digest with a singleton first, then a heavier centroid next
        Iterator<Centroid> ix = digest.centroids().iterator();
        Centroid first = ix.next();
        Centroid second = ix.next();
        assertEquals(1, first.count());
        assertEquals(0, first.mean(), 0);
//        assertTrue(second.count() > 1);
        assertEquals(1.0, second.mean(), 0);

        assertEquals(0.5 / digest.size(), digest.cdf(0), 0);
        assertEquals(1.0 / digest.size(), digest.cdf(1e-10), 1e-10);
        assertEquals(1.0 / digest.size(), digest.cdf(0.25), 1e-10);
    }

    /**
     * Make sure that the first and last centroids have unit weight
     */
    @Test
    public void testSingletonsAtEnds() {
        TDigest d = new MergingDigest(50);
        d.recordAllData();
        Random gen = new Random(1);
        double[] data = new double[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = Math.floor(gen.nextGaussian() * 3);
        }
        for (int i = 0; i < 100; i++) {
            for (double x : data) {
                d.add(x);
            }
        }
        int last = 0;
        for (Centroid centroid : d.centroids()) {
            if (last == 0) {
                assertEquals(1, centroid.count());
            }
            last = centroid.count();
        }
        assertEquals(1, last);
    }

    /**
     * Verify centroid sizes.
     */
    @Test
    public void testFill() {
        MergingDigest x = new MergingDigest(300);
        Random gen = new Random();
        ScaleFunction scale = x.getScaleFunction();
        double compression = x.compression();
        for (int i = 0; i < 1000000; i++) {
            x.add(gen.nextGaussian());
        }
        double q0 = 0;
        int i = 0;
        System.out.printf("i, q, mean, count, dk\n");
        for (Centroid centroid : x.centroids()) {
            double q = q0 + centroid.count() / 2.0 / x.size();
            double q1 = q0 + (double) centroid.count() / x.size();
            double dk = scale.k(q1, compression, x.size()) - scale.k(q0, compression, x.size());
            if (centroid.count() > 1) {
                assertTrue(String.format("K-size for centroid %d at %.3f is %.3f", i, centroid.mean(), dk), dk <= 1);
            } else {
                dk = 1;
            }
            System.out.printf("%d,%.7f,%.7f,%d,%.7f\n", i, q, centroid.mean(), centroid.count(), dk);
            if (Double.isNaN(dk)) {
                System.out.printf(">>>> %.8f, %.8f\n", q0, q1);
            }
            q0 = q1;
            i++;
        }
    }

    /**
     * Test with adversarial inputs.
     */
    @Test
    public void testAdversarial() throws FileNotFoundException {
        int kilo = 1000;
        Random gen = new Random();
        double maxE = Math.log(10) * 308;
        try (PrintWriter out = new PrintWriter("adversarial.csv")) {
            out.printf("k,n,E,q,x0,x1,q0,q1\n");
            for (int N : new int[]{100 * kilo, 1000 * kilo}) {
                System.out.printf("%d\n", N);
                double[] data = new double[N];
                for (double E : new double[]{10, 100, 300, 700, maxE}) {
                    TDigest digest = new MergingDigest(500);
                    for (int i = 0; i < N; i++) {
                        double u = gen.nextDouble();
                        data[i] = (gen.nextDouble() < 0.01 ? -1 : 1) * Math.exp((2 * u - 1) * E);
                        digest.add(data[i]);
                    }
                    Arrays.sort(data);

                    for (int k = 0; k < 10; k++) {
                        for (double q : new double[]{0, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 0.1, 0.5}) {
                            double x0 = Dist.quantile(q, data);
                            double x1 = digest.quantile(q);
                            double q0 = Dist.cdf(x0, data);
                            double q1 = Dist.cdf(x1, data);
                            out.printf("%d,%d,%.0f,%.6f,%.6g,%.6g,%.6f,%.6f\n", k, N, E, q, x0, x1, q0, q1);
                        }
                    }
                }
            }
        }
    }
}
