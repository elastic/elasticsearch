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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Base test case for TDigests, just extend this class and implement the abstract methods.
 */
public abstract class TDigestTests extends ESTestCase {

    public interface DigestFactory {
        TDigest create();
    }

    protected abstract DigestFactory factory(double compression);

    private DigestFactory factory() {
        return factory(100);
    }

    public void testBigJump() {
        TDigest digest = factory().create();
        for (int i = 1; i < 20; i++) {
            digest.add(i);
        }
        digest.add(1_000_000);

        assertEquals(10.5, digest.quantile(0.50), 1e-5);
        assertEquals(16.2, digest.quantile(0.80), 1e-5);
        assertEquals(18.1, digest.quantile(0.90), 1e-5);
        assertEquals(50_000, digest.quantile(0.95), 100);
        assertEquals(620_000, digest.quantile(0.98), 100);
        assertEquals(810_000, digest.quantile(0.99), 100);
        assertEquals(1_000_000, digest.quantile(1.00), 0);

        assertEquals(0.925, digest.cdf(19), 1e-11);
        assertEquals(0.95, digest.cdf(19.0000001), 1e-11);
        assertEquals(0.9, digest.cdf(19 - 0.0000001), 1e-11);

        digest = factory(80).create();
        digest.setScaleFunction(ScaleFunction.K_0);

        for (int j = 0; j < 100; j++) {
            for (int i = 1; i < 20; i++) {
                digest.add(i);
            }
            digest.add(1_000_000);
        }
        assertEquals(18.0, digest.quantile(0.885), 0.15);
        assertEquals(19.0, digest.quantile(0.915), 0.1);
        assertEquals(19.0, digest.quantile(0.935), 0.1);
        assertEquals(1_000_000.0, digest.quantile(0.965), 0.1);
    }

    public void testSmallCountQuantile() {
        List<Double> data = List.of(15.0, 20.0, 32.0, 60.0);
        TDigest td = factory(200).create();
        for (Double datum : data) {
            td.add(datum);
        }
        assertEquals(15.0, td.quantile(0.00), 1e-5);
        assertEquals(16.5, td.quantile(0.10), 1e-5);
        assertEquals(18.75, td.quantile(0.25), 1e-5);
        assertEquals(26.0, td.quantile(0.50), 1e-5);
        assertEquals(39.0, td.quantile(0.75), 1e-5);
        assertEquals(51.6, td.quantile(0.90), 1e-5);
        assertEquals(60.0, td.quantile(1.00), 1e-5);
    }

    public void testExplicitSkewedData() {
        double[] data = new double[] {
            245,
            246,
            247.249,
            240,
            243,
            248,
            250,
            241,
            244,
            245,
            245,
            247,
            243,
            242,
            241,
            50100,
            51246,
            52247,
            52249,
            51240,
            53243,
            59248,
            59250,
            57241,
            56244,
            55245,
            56245,
            575247,
            58243,
            51242,
            54241 };

        TDigest digest = factory().create();
        for (double x : data) {
            digest.add(x);
        }

        assertEquals(Dist.quantile(0.5, data), digest.quantile(0.5), 0);
    }

    public void testQuantile() {
        double[] samples = new double[] { 1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0 };

        TDigest hist1 = factory().create();
        List<Double> data = new ArrayList<>();

        for (int j = 0; j < 100; j++) {
            for (double x : samples) {
                data.add(x);
                hist1.add(x);
            }
        }
        TDigest hist2 = factory().create();
        hist1.compress();
        hist2.add(hist1);
        Collections.sort(data);
        hist2.compress();
        double x1 = hist1.quantile(0.5);
        double x2 = hist2.quantile(0.5);
        assertEquals(Dist.quantile(0.5, data), x1, 0.2);
        assertEquals(x1, x2, 0.01);
    }

    /**
     * Brute force test that cdf and quantile give reference behavior in digest made up of all singletons.
     */
    public void singletonQuantiles() {
        double[] data = new double[11];
        TDigest digest = factory().create();
        for (int i = 0; i < data.length; i++) {
            digest.add(i);
            data[i] = i;
        }

        for (double x = digest.getMin() - 0.1; x <= digest.getMax() + 0.1; x += 1e-3) {
            assertEquals(Dist.cdf(x, data), digest.cdf(x), 0);
        }

        for (int i = 0; i <= 1000; i++) {
            double q = 0.001 * i;
            double dist = Dist.quantile(q, data);
            double td = digest.quantile(q);
            assertEquals(String.valueOf(q), dist, td, 0.0);
        }
    }

    /**
     * Verifies behavior involving interpolation (or lack of same, really) between singleton centroids.
     */
    public void testSingleSingleRange() {
        TDigest digest = factory().create();
        digest.add(1);
        digest.add(2);
        digest.add(3);

        // verify the cdf is a step between singletons
        assertEquals(0.5 / 3.0, digest.cdf(1), 0);
        assertEquals(1 / 3.0, digest.cdf(1 + 1e-10), 0);
        assertEquals(1 / 3.0, digest.cdf(2 - 1e-10), 0);
        assertEquals(1.5 / 3.0, digest.cdf(2), 0);
        assertEquals(2 / 3.0, digest.cdf(2 + 1e-10), 0);
        assertEquals(2 / 3.0, digest.cdf(3 - 1e-10), 0);
        assertEquals(2.5 / 3.0, digest.cdf(3), 0);
        assertEquals(1.0, digest.cdf(3 + 1e-10), 0);
    }

    /**
     * Tests cases where min or max is not the same as the extreme centroid which has weight>1. In these cases min and
     * max give us a little information we wouldn't otherwise have.
     */
    public void testSingletonAtEnd() {
        TDigest digest = factory().create();
        digest.add(1);
        digest.add(2);
        digest.add(3);

        assertEquals(1, digest.getMin(), 0);
        assertEquals(3, digest.getMax(), 0);
        assertEquals(3, digest.centroidCount());
        assertEquals(0, digest.cdf(0), 0);
        assertEquals(0, digest.cdf(1 - 1e-9), 0);
        assertEquals(0.5 / 3, digest.cdf(1), 1e-10);
        assertEquals(1.0 / 3, digest.cdf(1 + 1e-10), 1e-10);
        assertEquals(2.0 / 3, digest.cdf(3 - 1e-9), 0);
        assertEquals(2.5 / 3, digest.cdf(3), 0);
        assertEquals(1.0, digest.cdf(3 + 1e-9), 0);

        digest.add(1);
        assertEquals(1.0 / 4, digest.cdf(1), 0);

        // normally min == mean[0] because weight[0] == 1
        // we can force this not to be true for testing
        digest = factory().create();
        digest.setScaleFunction(ScaleFunction.K_0);
        for (int i = 0; i < 100; i++) {
            digest.add(1);
            digest.add(2);
            digest.add(3);
        }
        // This sample would normally be added to the first cluster that already exists
        // but there is special code in place to prevent that centroid from ever
        // having weight of more than one
        // As such, near q=0, cdf and quantiles
        // should reflect this single sample as a singleton
        digest.add(0);
        assertTrue(digest.centroidCount() > 0);
        Centroid first = digest.centroids().iterator().next();
        assertEquals(1, first.count());
        assertEquals(first.mean(), digest.getMin(), 0.0);
        assertEquals(0.0, digest.getMin(), 0);
        assertEquals(0, digest.cdf(0 - 1e-9), 0);
        assertEquals(0.5 / digest.size(), digest.cdf(0), 1e-10);
        assertEquals(1.0 / digest.size(), digest.cdf(1e-9), 1e-10);

        assertEquals(0, digest.quantile(0), 0);
        assertEquals(0.5, digest.quantile(0.5 / digest.size()), 0.1);
        assertEquals(1.0, digest.quantile(1.0 / digest.size()), 0.1);
        assertEquals(first.mean(), 0.0, 1e-5);

        digest.add(4);
        Centroid last = digest.centroids().stream().reduce((prev, next) -> next).orElse(null);
        assertNotNull(last);
        assertEquals(1.0, last.count(), 0.0);
        assertEquals(4, last.mean(), 0);
        assertEquals(1.0, digest.cdf(digest.getMax() + 1e-9), 0);
        assertEquals(1 - 0.5 / digest.size(), digest.cdf(digest.getMax()), 0);
        assertEquals(1 - 1.0 / digest.size(), digest.cdf((digest.getMax() - 1e-9)), 1e-10);

        assertEquals(4, digest.quantile(1), 0);
        assertEquals(last.mean(), 4, 0);
    }

    /**
     * The example from issue #167
     */
    public void testIssue167() {
        TDigest d = factory().create();
        for (int i = 0; i < 2; ++i) {
            d.add(9000);
        }
        for (int i = 0; i < 11; ++i) {
            d.add(3000);
        }
        for (int i = 0; i < 26; ++i) {
            d.add(1000);
        }

        assertEquals(3000.0, d.quantile(0.90), 1e-5);
        assertEquals(3600.0, d.quantile(0.95), 1e-5);
        assertEquals(5880.0, d.quantile(0.96), 1e-5);
        assertEquals(8160.0, d.quantile(0.97), 1e-5);
        assertEquals(9000.0, d.quantile(0.98), 1e-5);
        assertEquals(9000.0, d.quantile(1.00), 1e-5);
    }

    public void testSingleValue() {
        Random rand = random();
        final TDigest digest = factory().create();
        final double value = rand.nextDouble() * 1000;
        digest.add(value);
        final double q = rand.nextDouble();
        for (double qValue : new double[] { 0, q, 1 }) {
            assertEquals(value, digest.quantile(qValue), 0.001f);
        }
    }

    public void testFewValues() {
        // When there are few values in the tree, quantiles should be exact
        final TDigest digest = factory().create();
        final Random r = random();
        final int length = r.nextInt(10);
        final List<Double> values = new ArrayList<>();
        for (int i = 0; i < length; ++i) {
            final double value;
            if (i == 0 || r.nextBoolean()) {
                value = r.nextDouble() * 100;
            } else {
                // introduce duplicates
                value = values.get(i - 1);
            }
            digest.add(value);
            values.add(value);
        }
        Collections.sort(values);

        // for this value of the compression, the tree shouldn't have merged any node
        assertEquals(digest.centroids().size(), values.size());
        for (double q : new double[] { 0, 1e-10, r.nextDouble(), 0.5, 1 - 1e-10, 1 }) {
            double q1 = Dist.quantile(q, values);
            double q2 = digest.quantile(q);
            assertEquals(String.format(Locale.ROOT, "At q=%g, expected %.2f vs %.2f", q, q1, q2), q1, q2, 0.03);
        }
    }

    public void testEmptyDigest() {
        TDigest digest = factory().create();
        assertEquals(0, digest.centroids().size());
        assertEquals(0, digest.centroids().size());
    }

    public void testEmpty() {
        final TDigest digest = factory().create();
        final double q = random().nextDouble();
        assertTrue(Double.isNaN(digest.quantile(q)));
    }

    public void testMoreThan2BValues() {
        final TDigest digest = factory().create();
        // carefully build a t-digest that is as if we added 3 uniform values from [0,1]
        double n = 3e9;
        double q0 = 0;
        for (int i = 0; i < 200 && q0 < 1 - 1e-10; ++i) {
            double k0 = digest.scale.k(q0, digest.compression(), n);
            double q = digest.scale.q(k0 + 1, digest.compression(), n);
            int m = (int) Math.max(1, n * (q - q0));
            digest.add((q + q0) / 2, m);
            q0 = q0 + m / n;
        }
        digest.compress();
        assertEquals(3_000_000_000L, digest.size());
        assertTrue(digest.size() > Integer.MAX_VALUE);
        final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1 };
        double prev = Double.NEGATIVE_INFINITY;
        for (double q : quantiles) {
            final double v = digest.quantile(q);
            assertTrue(String.format(Locale.ROOT, "q=%.1f, v=%.4f, pref=%.4f", q, v, prev), v >= prev);
            prev = v;
        }
    }

    public void testSorted() {
        final TDigest digest = factory().create();
        Random gen = random();
        for (int i = 0; i < 10000; ++i) {
            int w = 1 + gen.nextInt(10);
            double x = gen.nextDouble();
            for (int j = 0; j < w; j++) {
                digest.add(x);
            }
        }
        Centroid previous = null;
        for (Centroid centroid : digest.centroids()) {
            if (previous != null) {
                if (previous.mean() <= centroid.mean()) {
                    assertTrue(Double.compare(previous.mean(), centroid.mean()) <= 0);
                }
            }
            previous = centroid;
        }
    }

    public void testNaN() {
        final TDigest digest = factory().create();
        Random gen = random();
        final int iters = gen.nextInt(100);
        for (int i = 0; i < iters; ++i) {
            digest.add(gen.nextDouble(), 1 + gen.nextInt(10));
        }
        try {
            // both versions should fail
            if (gen.nextBoolean()) {
                digest.add(Double.NaN);
            } else {
                digest.add(Double.NaN, 1);
            }
            fail("NaN should be an illegal argument");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testMidPointRule() {
        TDigest dist = factory(200).create();
        dist.add(1);
        dist.add(2);

        double scale = 0;
        for (int i = 0; i < 1000; i++) {
            dist.add(1);
            dist.add(2);
            if (i % 8 == 0) {
                String message = String.format(Locale.ROOT, "i = %d", i);
                assertEquals(message, 0, dist.cdf(1 - 1e-9), 0);
                assertEquals(message, 0.25, dist.cdf(1), 0.01 * scale);
                assertEquals(message, 0.5, dist.cdf(1 + 1e-9), 0.03 * scale);
                assertEquals(message, 0.5, dist.cdf(2 - 1e-9), 0.03 * scale);
                assertEquals(message, 0.75, dist.cdf(2), 0.01 * scale);
                assertEquals(message, 1, dist.cdf(2 + 1e-9), 0);

                assertEquals(1.0, dist.quantile(0.0), 1e-5);
                assertEquals(1.0, dist.quantile(0.1), 1e-5);
                assertEquals(1.0, dist.quantile(0.2), 1e-5);

                assertTrue(dist.quantile(0.5) > 1.0);
                assertTrue(dist.quantile(0.5) < 2.0);

                assertEquals(2.0, dist.quantile(0.7), 1e-5);
                assertEquals(2.0, dist.quantile(0.8), 1e-5);
                assertEquals(2.0, dist.quantile(0.9), 1e-5);
                assertEquals(2.0, dist.quantile(1.0), 1e-5);
            }
            // this limit should be 70 for merging digest variants
            // I decreased it to help out AVLTreeDigest.
            // TODO fix AVLTreeDigest behavior
            if (i >= 39) {
                // when centroids start doubling up, accuracy is no longer perfect
                scale = 1;
            }
        }

    }

    public void testThreePointExample() {
        TDigest tdigest = factory().create();
        double x0 = 0.18615591526031494;
        double x1 = 0.4241943657398224;
        double x2 = 0.8813006281852722;

        tdigest.add(x0);
        tdigest.add(x1);
        tdigest.add(x2);

        double p10 = tdigest.quantile(0.1);
        double p50 = tdigest.quantile(0.5);
        double p90 = tdigest.quantile(0.9);
        double p95 = tdigest.quantile(0.95);
        double p99 = tdigest.quantile(0.99);

        assertTrue(Double.compare(p10, p50) <= 0);
        assertTrue(Double.compare(p50, p90) <= 0);
        assertTrue(Double.compare(p90, p95) <= 0);
        assertTrue(Double.compare(p95, p99) <= 0);

        assertEquals(x0, tdigest.quantile(0.0), 0);
        assertEquals(x2, tdigest.quantile(1.0), 0);

        assertTrue(String.valueOf(p10), Double.compare(x0, p10) < 0);
        assertTrue(String.valueOf(p10), Double.compare(x1, p10) > 0);
        assertTrue(String.valueOf(p99), Double.compare(x1, p99) < 0);
        assertTrue(String.valueOf(p99), Double.compare(x2, p99) > 0);
    }

    public void testSingletonInACrowd() {
        TDigest dist = factory().create();
        for (int i = 0; i < 10000; i++) {
            dist.add(10);
        }
        dist.add(20);
        dist.compress();

        // The actual numbers depend on how the digest get constructed.
        // A singleton on the right boundary yields much better accuracy, e.g. q(0.9999) == 10.
        // Otherwise, quantiles above 0.9 use interpolation between 10 and 20, thus returning higher values.
        assertEquals(10.0, dist.quantile(0), 0);
        assertEquals(10.0, dist.quantile(0.9), 0);
        assertEquals(19.0, dist.quantile(0.99999), 1);
        assertEquals(20.0, dist.quantile(1), 0);
    }

    public void testScaling() {
        final Random gen = random();

        List<Double> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(gen.nextDouble());
        }
        Collections.sort(data);

        for (double compression : new double[] { 10, 20, 50, 100, 200, 500, 1000 }) {
            TDigest dist = factory(compression).create();
            for (Double x : data) {
                dist.add(x);
            }
            dist.compress();

            for (double q : new double[] { 0.001, 0.01, 0.1, 0.5 }) {
                double estimate = dist.quantile(q);
                double actual = data.get((int) (q * data.size()));
                if (Double.compare(estimate, 0) != 0) {
                    assertTrue(Double.compare(Math.abs(actual - estimate) / estimate, 1) < 0);
                } else {
                    assertEquals(Double.compare(estimate, 0), 0);
                }
            }
        }
    }

    public void testMonotonicity() {
        TDigest digest = factory().create();
        final Random gen = random();
        for (int i = 0; i < 100000; i++) {
            digest.add(gen.nextDouble());
        }

        double lastQuantile = -1;
        double lastX = -1;
        for (double z = 0; z <= 1; z += 1e-4) {
            double x = digest.quantile(z);
            assertTrue("q: " + z + " x: " + x + " last: " + lastX, Double.compare(x, lastX) >= 0);
            lastX = x;

            double q = digest.cdf(z);
            assertTrue("Q: " + z, Double.compare(q, lastQuantile) >= 0);
            lastQuantile = q;
        }
    }
}
