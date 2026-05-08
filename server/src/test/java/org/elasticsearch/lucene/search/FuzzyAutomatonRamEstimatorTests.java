/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.elasticsearch.test.ESTestCase;

public class FuzzyAutomatonRamEstimatorTests extends ESTestCase {

    public void testZeroEditsReturnsZero() {
        assertEquals(0L, FuzzyAutomatonRamEstimator.estimate(0, 0, 0, 0, 0));
        assertEquals(0L, FuzzyAutomatonRamEstimator.estimate(50, 50, 1, 0, 0));
        assertEquals(0L, FuzzyAutomatonRamEstimator.estimate(50, 50, 1, 0, 5));
    }

    public void testEstimateIsMonotonicInTermLength() {
        long shorter = FuzzyAutomatonRamEstimator.estimate(10, 10, 10, 2, 0);
        long longer = FuzzyAutomatonRamEstimator.estimate(50, 50, 10, 2, 0);
        long longest = FuzzyAutomatonRamEstimator.estimate(200, 200, 10, 2, 0);
        assertTrue(shorter < longer);
        assertTrue(longer < longest);
    }

    public void testEstimateIsMonotonicInMaxEdits() {
        long e1 = FuzzyAutomatonRamEstimator.estimate(20, 20, 20, 1, 0);
        long e2 = FuzzyAutomatonRamEstimator.estimate(20, 20, 20, 2, 0);
        assertTrue("expected estimate to grow with maxEdits", e1 < e2);
    }

    public void testEstimateIsMonotonicInDistinctUtf8Bytes() {
        long narrow = FuzzyAutomatonRamEstimator.estimate(20, 60, 1, 2, 0);
        long medium = FuzzyAutomatonRamEstimator.estimate(20, 60, 5, 2, 0);
        long wide = FuzzyAutomatonRamEstimator.estimate(20, 60, 60, 2, 0);
        assertTrue("distinct alphabet should grow the estimate", narrow < medium);
        assertTrue("distinct alphabet should grow the estimate", medium < wide);
    }

    public void testPrefixLengthShrinksEstimate() {
        long noPrefix = FuzzyAutomatonRamEstimator.estimate(20, 60, 60, 2, 0);
        long withPrefix = FuzzyAutomatonRamEstimator.estimate(20, 60, 60, 2, 5);
        assertTrue("prefix should reduce the suffix-driven cost", withPrefix < noPrefix);
    }

    public void testPrefixLongerThanTermIsClampedNotNegative() {
        long allPrefix = FuzzyAutomatonRamEstimator.estimate(5, 5, 5, 2, 100);
        long zeroLen = FuzzyAutomatonRamEstimator.estimate(0, 0, 0, 2, 0);
        assertEquals("prefix >= termLength should behave like termLength == 0", zeroLen, allPrefix);
    }

    public void testEstimateRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(-1, 0, 0, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(10, -1, 0, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(10, 10, -1, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(10, 10, 10, -1, 0));
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(10, 10, 10, 1, -1));
    }

    public void testEstimateRejectsMaxEditsAboveLuceneLimit() {
        expectThrows(IllegalArgumentException.class, () -> FuzzyAutomatonRamEstimator.estimate(10, 10, 10, 99, 0));
    }
}
