/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.elasticsearch.test.ESTestCase;

import java.util.SplittableRandom;

import static org.hamcrest.Matchers.closeTo;

public class FastGeometricTests extends ESTestCase {

    private static final int N = 10_000_000;
    private static final double[] PROBABILITIES = new double[] { 0.5, 0.1, 0.01, 0.001, 0.0001, 0.00001 };

    public void testGeometricSeries() {
        for (double p : PROBABILITIES) {
            final SplittableRandom rng = new SplittableRandom(randomLong());
            final int size = 32;

            double[] expected = new double[size];
            for (int k = 0; k < size; k++) {
                expected[k] = Math.pow(1.0 - p, k) * p;
            }

            FastGeometric geometric = new FastGeometric(rng::nextInt, p);
            int[] counts = new int[size];
            double mean = 0.0;
            for (int i = 0; i < N; ++i) {
                int sample = (geometric.next() - 1);
                if (sample < counts.length) {
                    counts[sample]++;
                }
                mean += sample;
            }
            double[] fractions = new double[counts.length];
            for (int i = 0; i < counts.length; ++i) {
                fractions[i] = (double) (counts[i]) / (double) (N);
            }
            for (int i = 0; i < size; i++) {
                assertThat("inaccurate geometric sampling at probability [" + p + "]", fractions[i], closeTo(expected[i], 1e-2));
            }
            mean /= N;
            double expectedMean = (1 - p) / p;
            assertThat("biased mean value when sampling at probability [" + p + "]", mean, closeTo(expectedMean, 1.0 / (Math.pow(p, 0.5))));
        }
    }

}
