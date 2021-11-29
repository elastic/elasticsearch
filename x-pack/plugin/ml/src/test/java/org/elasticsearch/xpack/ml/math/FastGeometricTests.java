/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.math;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class FastGeometricTests extends ESTestCase {

    private static final int N = 10_000_000;
    private static final double[] PROBABILITIES = new double[] { 0.5, 0.1, 0.01, 0.001 };

    public void testGeometricSeries() {
        final PCG rng = new PCG(randomLong());
        for (double p : PROBABILITIES) {
            final int size = 32;

            double[] expected = new double[size];
            for (int k = 0; k < size; k++) {
                expected[k] = Math.pow(1.0 - p, k) * p;
            }

            FastGeometric geometric = new FastGeometric(rng::nextInt, p);
            int[] counts = new int[size];
            for (int i = 0; i < N; ++i) {
                int sample = geometric.next();
                if (sample < counts.length) {
                    counts[sample]++;
                }
            }
            double[] fractions = new double[counts.length];
            for (int i = 0; i < counts.length; ++i) {
                fractions[i] = (double) (counts[i]) / (double) (N);
            }
            for (int i = 0; i < size; i++) {
                assertThat("inaccurate geometric sampling at probability [" + p + "]", fractions[i], closeTo(expected[i], 1e-2));
            }
        }
    }

}
