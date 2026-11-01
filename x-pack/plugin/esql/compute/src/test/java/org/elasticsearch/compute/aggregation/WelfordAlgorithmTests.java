/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.test.ESTestCase;

/** Tests that merging partial results preserves the variance of the input values. */
public class WelfordAlgorithmTests extends ESTestCase {

    /** Identical values must retain zero variance even when their partial means are large. */
    public void testCombineConstantValues() {
        for (double value : new double[] { -5118973529465149924L, 5118973529465149924L, Double.MAX_VALUE, -Double.MAX_VALUE }) {
            var combined = new WelfordAlgorithm();
            // These partition sizes reproduce rounding in the weighted sum of the first two means.
            for (int count : new int[] { 2, 15, 4 }) {
                var partial = new WelfordAlgorithm();
                for (int i = 0; i < count; i++) {
                    partial.add(value);
                }
                combined.add(partial.mean(), partial.m2(), partial.count());
            }
            assertEquals(0.0, combined.evaluate(true), 0.0);
            assertEquals(0.0, combined.evaluate(false), 0.0);
            assertEquals(value, combined.mean(), 0.0);
            assertEquals(21L, combined.count());
        }
    }

    /** Unequal partition sizes must weight both the mean and the variance correctly. */
    public void testCombineDifferentValues() {
        var combined = new WelfordAlgorithm();
        for (double[] values : new double[][] { { 2, 4, 4 }, { 4, 5, 5, 7, 9 } }) {
            var partial = new WelfordAlgorithm();
            for (double value : values) {
                partial.add(value);
            }
            combined.add(partial.mean(), partial.m2(), partial.count());
        }
        assertEquals(8L, combined.count());
        assertEquals(5.0, combined.mean(), 1e-12);
        assertEquals(4.0, combined.evaluate(false), 1e-12);
        assertEquals(2.0, combined.evaluate(true), 1e-12);
    }
}
