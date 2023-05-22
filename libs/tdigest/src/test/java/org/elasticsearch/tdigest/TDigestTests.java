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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

//import org.apache.commons.math3.distribution.AbstractRealDistribution;
//import org.apache.commons.math3.distribution.GammaDistribution;

/**
 * Base test case for TDigests, just extend this class and implement the abstract methods.
 */
public abstract class TDigestTests extends ESTestCase {

    private static String digestName;

    public static void setup(String digestName) throws IOException {
        TDigestTests.digestName = digestName;
    }

    public interface DigestFactory {
        TDigest create();
    }

    protected abstract DigestFactory factory(double compression);

    private DigestFactory factory() {
        return factory(100);
    }

    public void testOffsetUniform() {
        for (double compression : new double[] { 20, 50, 100, 200 }) {
            TDigest digest = factory(compression).create();
            digest.setScaleFunction(ScaleFunction.K_0);
            Random rand = random();
            double[] data = new double[1_000_000];
            for (int i = 0; i < 1_000_000; i++) {
                data[i] = 50.0 + rand.nextDouble();
                digest.add(data[i]);
            }
            Arrays.sort(data);
        }
    }

    public void testBigJump() {
        TDigest digest = factory(100).create();
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
        assertEquals(18.0, digest.quantile(0.885), 0.0);
        assertEquals(19.0, digest.quantile(0.915), 0.0);
        assertEquals(19.0, digest.quantile(0.935), 0.0);
        assertEquals(1_000_000.0, digest.quantile(0.965), 0.0);
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

    /**
     * Check against the example given in
     * https://github.com/tdunning/t-digest/issues/143
     * <p>
     * Don't think that there is a problem here, but keeping the test just in case.
     */
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

        TDigest digest = factory(50).create();
        for (double x : data) {
            digest.add(x);
        }

        assertEquals(Dist.quantile(0.5, data), digest.quantile(0.5), 0);
    }

    /**
     * See https://github.com/tdunning/t-digest/issues/114
     * <p>
     * The problem in that issue seems to have been due to adding samples with non-unit weight.
     * This resulted in a violation of the t-digest invariant.
     * <p>
     * The question in the issue about the origin of the shuffle still applies.
     */
    public void testQuantile() {
        double compression = 100;
        double[] samples = new double[] { 1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0 };
        for (int i = 1; i < 10000; i++) {
            TDigest hist1 = new MergingDigest(compression);
            List<Double> data = new ArrayList<>();

            for (int j = 0; j < 100; j++) {
                for (double x : samples) {
                    data.add(x);
                    hist1.add(x);
                }
            }
            TDigest hist2 = new MergingDigest(compression);
            hist1.compress();
            hist2.add(hist1);
            Collections.sort(data);
            hist2.compress();
            double x1 = hist1.quantile(0.5);
            double x2 = hist2.quantile(0.5);
            assertEquals(Dist.quantile(0.5, data), x1, 0.2);
            assertEquals(x1, x2, 0.01);
        }
    }

    /**
     * Brute force test that cdf and quantile give reference behavior in digest made up of all singletons.
     */
    public void singletonQuantiles() {
        double[] data = new double[11];
        TDigest digest = factory(100).create();
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
        TDigest digest = factory(100).create();
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
        MergingDigest digest = new MergingDigest(100);
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
        digest = new MergingDigest(1);
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
        TDigest d = factory(100).create();
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

    // private int repeats() {
    // return Boolean.parseBoolean(System.getProperty("runSlowTests")) ? 10 : 1;
    // }

    public void testEmptyDigest() {
        TDigest digest = factory().create();
        assertEquals(0, digest.centroids().size());
        assertEquals(0, digest.centroids().size());
    }

    // /**
    // * Builds estimates of the CDF of a bunch of data points and checks that the centroids are accurately positioned.
    // * Accuracy is assessed in terms of the estimated CDF which is much more stringent than checking position of
    // * quantiles with a single value for desired accuracy.
    // *
    // * @param gen Random number generator that generates desired values.
    // * @param tag Label for the output lines
    // * @param recordAllData True if the internal histogrammer should be set up to record all data it sees for
    // */
    // private void runTest(DigestFactory factory, AbstractRealDistribution gen, double[] qValues, String tag, boolean recordAllData) {
    // TDigest dist = factory.create();
    // if (recordAllData) {
    // dist.recordAllData();
    // }
    //
    // double[] data = new double[100000];
    // for (int i = 0; i < 100000; i++) {
    // data[i] = gen.sample();
    // }
    // long t0 = System.nanoTime();
    // for (double x : data) {
    // dist.add(x);
    // }
    // Arrays.sort(data);
    //
    // double[] xValues = qValues.clone();
    // for (int i = 0; i < qValues.length; i++) {
    // xValues[i] = Dist.quantile(qValues[i], data);
    // }
    //
    // double qz = 0;
    // int iz = 0;
    // for (Centroid centroid : dist.centroids()) {
    // double q = (qz + centroid.count() / 2.0) / dist.size();
    // qz += centroid.count();
    // iz++;
    // }
    // assertEquals(iz, dist.centroids().size());
    // dist.compress();
    // assertEquals(qz, dist.size(), 1e-10);
    //
    // assertTrue(
    // String.format("Summary is too large (got %d, wanted <= %.1f)", dist.centroids().size(), dist.compression()),
    // dist.centroids().size() <= dist.compression()
    // );
    // int softErrors = 0;
    // for (int i = 0; i < xValues.length; i++) {
    // double x = xValues[i];
    // double q = qValues[i];
    // double estimate = dist.cdf(x);
    // assertEquals(q, estimate, 0.08);
    //
    // estimate = Dist.cdf(dist.quantile(q), data);
    // if (Math.abs(q - estimate) > 0.005) {
    // softErrors++;
    // }
    // assertEquals(String.format("discrepancy %.5f vs %.5f @ %.5f", q, estimate, x), q, estimate, 0.012);
    // }
    // assertTrue(softErrors < 3);
    //
    // if (recordAllData) {
    // Iterator<? extends Centroid> ix = dist.centroids().iterator();
    // Centroid b = ix.next();
    // Centroid c = ix.next();
    // qz = b.count();
    // while (ix.hasNext()) {
    // Centroid a = b;
    // b = c;
    // c = ix.next();
    // double left = (b.mean() - a.mean()) / 2;
    // double right = (c.mean() - b.mean()) / 2;
    //
    // double q = (qz + b.count() / 2.0) / dist.size();
    // qz += a.count();
    // }
    // }
    // }

    public void testEmpty() {
        final TDigest digest = factory().create();
        final double q = random().nextDouble();
        assertTrue(Double.isNaN(digest.quantile(q)));
    }

    public void testMoreThan2BValues() {
        final TDigest digest = factory(100).create();
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

    // public void testUniform() {
    // Random gen = random();
    // for (int i = 0; i < repeats(); i++) {
    // runTest(factory(), gen, new double[] { 0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999 }, "uniform", true);
    // }
    // }
    //

    // public void testGamma() {
    // // this Gamma distribution is very heavily skewed. The 0.1%-ile is 6.07e-30 while
    // // the median is 0.006 and the 99.9th %-ile is 33.6 while the mean is 1.
    // // this severe skew means that we have to have positional accuracy that
    // // varies by over 11 orders of magnitude.
    // Random gen = random();
    // for (int i = 0; i < repeats(); i++) {
    // runTest(
    // factory(200),
    // new GammaDistribution(0.1, 0.1),
    // // new double[]{6.0730483624079e-30, 6.0730483624079e-20, 6.0730483627432e-10, 5.9339110446023e-03,
    // // 2.6615455373884e+00, 1.5884778179295e+01, 3.3636770117188e+01},
    // new double[] { 0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999 },
    // "gamma",
    // true
    // );
    // }
    // }

    // public void testNarrowNormal() {
    // // this mixture of a uniform and normal distribution has a very narrow peak which is centered
    // // near the median. Our system should be scale invariant and work well regardless.
    // final Random gen = random();
    // AbstractContinousDistribution mix = new AbstractContinousDistribution() {
    // final AbstractContinousDistribution normal = new Normal(0, 1e-5, gen);
    // final AbstractContinousDistribution uniform = new Uniform(-1, 1, gen);
    //
    // @Override
    // public double nextDouble() {
    // double x;
    // if (gen.nextDouble() < 0.5) {
    // x = uniform.nextDouble();
    // } else {
    // x = normal.nextDouble();
    // }
    // return x;
    // }
    // };
    //
    // for (int i = 0; i < repeats(); i++) {
    // runTest(factory(400), mix, new double[] { 0.001, 0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99, 0.999 }, "mixture", false);
    // }
    // }

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

    // public void testRepeatedValues() {
    // final Random gen = random();
    //
    // // 5% of samples will be 0 or 1.0. 10% for each of the values 0.1 through 0.9
    // AbstractContinousDistribution mix = new AbstractContinousDistribution() {
    // @Override
    // public double nextDouble() {
    // return Math.rint(gen.nextDouble() * 10) / 10.0;
    // }
    // };
    //
    // TDigest dist = factory(400).create();
    // List<Double> data = Lists.newArrayList();
    // for (int i1 = 0; i1 < 1000000; i1++) {
    // double x = mix.nextDouble();
    // data.add(x);
    // }
    //
    // long t0 = System.nanoTime();
    // for (double x : data) {
    // dist.add(x);
    // }
    //
    // assertTrue("Summary is too large: " + dist.centroids().size(), dist.centroids().size() < dist.compression());
    //
    // // all quantiles should round to nearest actual value
    // for (int i = 0; i < 10; i++) {
    // double z = i / 10.0;
    // // we skip over troublesome points that are nearly halfway between
    // for (double delta : new double[] { 0.01, 0.02, 0.03, 0.07, 0.08, 0.09 }) {
    // double q = z + delta;
    // double cdf = dist.cdf(q);
    // // we also relax the tolerances for repeated values
    // assertEquals(String.format("z=%.1f, q = %.3f, cdf = %.3f", z, q, cdf), z + 0.05, cdf, 0.03);
    //
    // double estimate = dist.quantile(q);
    // assertEquals(
    // String.format("z=%.1f, q = %.3f, cdf = %.3f, estimate = %.3f", z, q, cdf, estimate),
    // Math.rint(q * 10) / 10.0,
    // estimate,
    // 0.02
    // );
    // }
    // }
    // }

    // public void testSequentialPoints() {
    // for (int i = 0; i < repeats(); i++) {
    // runTest(factory(), new AbstractContinousDistribution() {
    // double base = 0;
    //
    // @Override
    // public double nextDouble() {
    // base += Math.PI * 1e-5;
    // return base;
    // }
    // }, new double[] { 0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999 }, "sequential", true);
    // }
    // }

    /**
     * Does basic sanity testing for a particular small example that used to fail. See
     * https://github.com/addthis/stream-lib/issues/138
     */
    public void testThreePointExample() {
        TDigest tdigest = factory(100).create();
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
        TDigest dist = factory(100).create();
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
        assertEquals(10.0, dist.quantile(0.999), 3);
        assertEquals(12.0, dist.quantile(0.9999), 3);
        assertEquals(19.0, dist.quantile(0.99999), 3);
        assertEquals(20.0, dist.quantile(1), 0);
    }

    public void testSizeControl() throws ExecutionException, InterruptedException {
        List<Callable<String>> tasks = new ArrayList<>();
        for (final int size : new int[] { 10, 100, 1000, 10000 }) {
            tasks.add(new Callable<>() {
                @Override
                public String call() {
                    final Random gen = random();
                    for (double compression : new double[] { 50, 100, 200, 500 }) {
                        TDigest dist = factory(compression).create();
                        for (int i = 0; i < size * 1000; i++) {
                            dist.add(gen.nextDouble() * size);
                        }
                        dist.compress();
                    }
                    return "done";
                }
            });
        }
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 2);
        for (Future<String> result : executor.invokeAll(tasks)) {
            assertEquals("done", result.get());
        }
        executor.shutdownNow();
        assertTrue("Dangling executor thread", executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testScaling() {
        final Random gen = random();
        for (int k = 0; k < 10; k++) {
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
                    if (Double.compare(estimate, 0) != 0) assertTrue(Double.compare(Math.abs(actual - estimate) / estimate, 1) < 0);
                    else assertEquals(Double.compare(estimate, 0), 0);
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
        for (double z = 0; z <= 1; z += 1e-5) {
            double x = digest.quantile(z);
            assertTrue("q: " + z + " x: " + x + " last: " + lastX, Double.compare(x, lastX) >= 0);
            lastX = x;

            double q = digest.cdf(z);
            assertTrue("Q: " + z, Double.compare(q, lastQuantile) >= 0);
            lastQuantile = q;
        }
    }

    // public void testKSDrift() {
    // final Random gen = random();
    // int N1 = 50;
    // int N2 = 10000;
    // double[] data = new double[N1 * N2];
    // for (int rep = 0; rep < 5; rep++) {
    // TDigest digest = factory(200).create();
    // for (int i = 0; i < N1; i++) {
    // for (int j = 0; j < N2; j++) {
    // double x = gen.nextDouble();
    // data[i * N2 + j] = x;
    // digest.add(x);
    // }
    // digest.centroidCount());
    // }
    // }
    // }

    // private double ks(double[] data, int length, TDigest digest) {
    // double d1 = 0;
    // double d2 = 0;
    // Arrays.sort(data, 0, length);
    // int i = 0;
    // for (Centroid centroid : digest.centroids()) {
    // double x = centroid.mean();
    // while (i < length && data[i] <= x) {
    // i++;
    // }
    // double q0a = (double) i / (length - 1);
    // double q0b = (double) (i + 1) / (length - 1);
    // double q0;
    // if (i > 0) {
    // if (i < length) {
    // q0 = (q0a * (data[i] - x) + q0b * (x - data[i - 1])) / (data[i] - data[i - 1]);
    // } else {
    // q0 = 1;
    // }
    // } else {
    // q0 = 0;
    // }
    // double q1 = digest.cdf(x);
    // d1 = Math.max(q1 - q0, d1);
    // d2 = Math.max(q0 - q1, d2);
    // }
    // return Math.max(d1, d2);
    // }
}
