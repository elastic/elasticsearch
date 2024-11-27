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

import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.function.Supplier;

public class ComparisonTests extends TDigestTestCase {

    private static final int SAMPLE_COUNT = 1_000_000;

    private TDigest avlTreeDigest;
    private TDigest mergingDigest;
    private TDigest sortingDigest;
    private TDigest hybridDigest;

    double[] samples;

    private void loadData(Supplier<Double> sampleGenerator) {
        final int COMPRESSION = 100;
        avlTreeDigest = TDigest.createAvlTreeDigest(arrays(), COMPRESSION);
        mergingDigest = TDigest.createMergingDigest(arrays(), COMPRESSION);
        sortingDigest = TDigest.createSortingDigest(arrays());
        hybridDigest = TDigest.createHybridDigest(arrays(), COMPRESSION);
        samples = new double[SAMPLE_COUNT];

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = sampleGenerator.get();
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
            sortingDigest.add(samples[i]);
            hybridDigest.add(samples[i]);
        }
        Arrays.sort(samples);
    }

    private void releaseData() {
        Releasables.close(avlTreeDigest, mergingDigest, sortingDigest, hybridDigest);
    }

    public void testRandomDenseDistribution() {
        loadData(() -> random().nextDouble());

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, sortingDigest.quantile(q), 0);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, hybridDigest.quantile(q), accuracy);
        }

        releaseData();
    }

    public void testRandomSparseDistribution() {
        loadData(() -> random().nextDouble() * SAMPLE_COUNT * SAMPLE_COUNT + SAMPLE_COUNT);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, sortingDigest.quantile(q), 0);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, hybridDigest.quantile(q), accuracy);
        }

        releaseData();
    }

    public void testDenseGaussianDistribution() {
        loadData(() -> random().nextGaussian());

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, sortingDigest.quantile(q), 0);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, hybridDigest.quantile(q), accuracy);
        }

        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, sortingDigest.quantile(0.5), 0);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 0.01);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 0.01);
        assertEquals(expectedMedian, hybridDigest.quantile(0.5), 0.01);

        releaseData();
    }

    public void testSparseGaussianDistribution() {
        loadData(() -> random().nextGaussian() * SAMPLE_COUNT);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, sortingDigest.quantile(q), 0);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, hybridDigest.quantile(q), accuracy);
        }

        // The absolute value of median is within [0,5000], which is deemed close enough to 0 compared to the max value.
        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, sortingDigest.quantile(0.5), 0);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 5000);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 5000);
        assertEquals(expectedMedian, hybridDigest.quantile(0.5), 5000);

        releaseData();
    }
}
