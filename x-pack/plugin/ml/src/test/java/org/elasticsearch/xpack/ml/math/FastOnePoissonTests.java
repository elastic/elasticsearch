/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.math;

import org.elasticsearch.test.ESTestCase;

import java.util.SplittableRandom;

import static org.hamcrest.Matchers.closeTo;

public class FastOnePoissonTests extends ESTestCase {

    private static final int N = 10_000_000;

    public void testOnePoissonSeries() {
        final SplittableRandom rng = new SplittableRandom(randomLong());
        final int size = 12;

        double[] expected = new double[size];
        double f = Math.exp(-1.0);
        for (int k = 0; k < size;) {
            expected[k] = Math.floor((double) N * f + 0.5) / (double) N;
            f /= ++k;
        }

        FastOnePoisson poisson = new FastOnePoisson(rng);
        int[] counts = new int[size];
        double mean = 0.0;
        for (int i = 0; i < N; ++i) {
            int sample = poisson.next();
            if (sample < counts.length) {
                counts[sample]++;
            }
            mean += sample;
        }
        mean /= N;
        double[] fractions = new double[counts.length];
        for (int i = 0; i < counts.length; ++i) {
            fractions[i] = (double) (counts[i]) / (double) (N);
        }
        for (int i = 0; i < size; i++) {
            assertThat(fractions[i], closeTo(expected[i], 1e-4));
        }
        assertThat(mean, closeTo(1.0, 1e-4));
    }

}
