/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

abstract class SamplingMethodTests extends ESTestCase {

    private static int NUM_TEST_RUNS = 10;

    abstract SamplingMethod createInstance();

    abstract boolean isDescending();

    public void testMonotonic() {
        SamplingMethod method = createInstance();
        double[] cdfs = method.cdfPoints();
        double lft = cdfs[0];
        for (int i = 1; i < cdfs.length; i++) {
            assertThat(
                "failed monotonic test [" + (isDescending() ? "desc" : "asc") + "] at point [" + i + "]",
                lft,
                isDescending() ? greaterThan(cdfs[i]) : lessThan(cdfs[i])
            );
            lft = cdfs[i];
        }
    }

    public void testAllPositive() {
        SamplingMethod method = createInstance();
        double[] cdfs = method.cdfPoints();
        for (int i = 0; i < cdfs.length; i++) {
            assertThat(cdfs[i], greaterThan(0.0));
        }
    }

    public void testConsistent() {
        for (int j = 0; j < NUM_TEST_RUNS; j++) {
            SamplingMethod lft = createInstance();
            SamplingMethod rgt = createInstance();
            assertArrayEquals(lft.cdfPoints(), rgt.cdfPoints(), 0.0);
        }
    }
}
