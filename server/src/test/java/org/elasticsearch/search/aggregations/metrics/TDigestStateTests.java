/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class TDigestStateTests extends ESTestCase {

    public void testMoreThan4BValues() {
        // Regression test for #19528
        // See https://github.com/tdunning/t-digest/pull/70/files#diff-4487072cee29b939694825647928f742R439
        TDigestState digest = new TDigestState(100);
        for (int i = 0; i < 1000; ++i) {
            digest.add(randomDouble());
        }
        final int count = 1 << 29;
        for (int i = 0; i < 10; ++i) {
            digest.add(randomDouble(), count);
        }
        assertEquals(1000 + 10L * (1 << 29), digest.size());
        assertTrue(digest.size() > 2 * Integer.MAX_VALUE);
        final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1, randomDouble() };
        Arrays.sort(quantiles);
        double prev = Double.NEGATIVE_INFINITY;
        for (double q : quantiles) {
            final double v = digest.quantile(q);
            logger.trace("q=" + q + ", v=" + v);
            assertTrue(v >= prev);
            assertTrue("Unexpectedly low value: " + v, v >= 0.0);
            assertTrue("Unexpectedly high value: " + v, v <= 1.0);
            prev = v;
        }
    }

    // NOCOMMIT: put a real test name here
    public void testSomeStuff() {
        int size = 7;
        TDigestState[] states = new TDigestState[size];
        for (int counter = 0; counter < size; counter++) {
            states[counter] = new TDigestState(50);
            final int numberDocs = randomIntBetween(5, 50);
            for (int i = 0; i < numberDocs; i++) {
                double value = randomDoubleBetween(-1000, 1000, true);
                states[counter].add(value);
            }
        }

        int fromIndex = 0;
        int toIndex = 6;
        TDigestState result = new TDigestState(states[0].compression());
        for (int i = fromIndex; i < toIndex; i++) {
            result.add(states[i]);
        }
        assertNotEquals(Double.NEGATIVE_INFINITY, result.getMax());
        assertTrue(result.getMax() > result.getMin());
    }
}
