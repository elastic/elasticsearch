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

public class ComparisonTests extends ESTestCase {

    public void testRandomDenseDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextDouble();
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }
    }

    public void testRandomSparseDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextDouble() * SAMPLE_COUNT * SAMPLE_COUNT + SAMPLE_COUNT;
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }
    }

    public void testDenseGaussianDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextGaussian();
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }

        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 0.01);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 0.01);
    }

    public void testSparseGaussianDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];
        var rand = random();

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextGaussian() * SAMPLE_COUNT;
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }

        // The absolute value of median is within [0,5000], which is deemed close enough to 0 compared to the max value.
        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 5000);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 5000);
    }
}
