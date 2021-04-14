/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

public class CompensatedSumTests extends ESTestCase {

    /**
     * When adding a series of numbers the order of the numbers should not impact the results.
     *
     * <p>This test shows that a naive summation comes up with a different result than Kahan
     * Summation when you start with either a smaller or larger number in some cases and
     * helps prove our Kahan Summation is working.
     */
    public void testAdd() {
        final CompensatedSum smallSum = new CompensatedSum(0.001, 0.0);
        final CompensatedSum largeSum = new CompensatedSum(1000, 0.0);

        CompensatedSum compensatedResult1 = new CompensatedSum(0.001, 0.0);
        CompensatedSum compensatedResult2 = new CompensatedSum(1000, 0.0);
        double naiveResult1 = smallSum.value();
        double naiveResult2 = largeSum.value();

        for (int i = 0; i < 10; i++) {
            compensatedResult1.add(smallSum.value());
            compensatedResult2.add(smallSum.value());
            naiveResult1 += smallSum.value();
            naiveResult2 += smallSum.value();
        }

        compensatedResult1.add(largeSum.value());
        compensatedResult2.add(smallSum.value());
        naiveResult1 += largeSum.value();
        naiveResult2 += smallSum.value();

        // Kahan summation gave the same result no matter what order we added
        Assert.assertEquals(1000.011, compensatedResult1.value(), 0.0);
        Assert.assertEquals(1000.011, compensatedResult2.value(), 0.0);

        // naive addition gave a small floating point error
        Assert.assertEquals(1000.011, naiveResult1, 0.0);
        Assert.assertEquals(1000.0109999999997, naiveResult2, 0.0);

        Assert.assertEquals(compensatedResult1.value(), compensatedResult2.value(), 0.0);
        Assert.assertEquals(naiveResult1, naiveResult2, 0.0001);
        Assert.assertNotEquals(naiveResult1, naiveResult2, 0.0);
    }

    public void testDelta() {
        CompensatedSum compensatedResult1 = new CompensatedSum(0.001, 0.0);
        for (int i = 0; i < 10; i++) {
            compensatedResult1.add(0.001);
        }

        Assert.assertEquals(0.011, compensatedResult1.value(), 0.0);
        Assert.assertEquals(Double.parseDouble("8.673617379884035E-19"), compensatedResult1.delta(), 0.0);
    }

    public void testInfiniteAndNaN() {
        CompensatedSum compensatedResult1 = new CompensatedSum(0, 0);
        double[] doubles = {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN};
        for (double d : doubles) {
            compensatedResult1.add(d);

        }

        Assert.assertTrue(Double.isNaN(compensatedResult1.value()));
    }
}
