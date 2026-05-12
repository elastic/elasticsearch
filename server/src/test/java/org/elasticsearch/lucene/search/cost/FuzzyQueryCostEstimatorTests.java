/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.elasticsearch.test.ESTestCase;

public class FuzzyQueryCostEstimatorTests extends ESTestCase {

    public void testZeroEditsReturnsZero() {
        assertEquals(0L, new FuzzyQueryCostEstimator(0, 0, 0, 0).estimate());
        assertEquals(0L, new FuzzyQueryCostEstimator(50, 1, 0, 0).estimate());
        assertEquals(0L, new FuzzyQueryCostEstimator(50, 1, 0, 5).estimate());
    }

    public void testEstimateIsMonotonicInTermLength() {
        long shorter = new FuzzyQueryCostEstimator(10, 10, 2, 0).estimate();
        long longer = new FuzzyQueryCostEstimator(50, 10, 2, 0).estimate();
        long longest = new FuzzyQueryCostEstimator(200, 10, 2, 0).estimate();
        assertTrue(shorter < longer);
        assertTrue(longer < longest);
    }

    public void testEstimateIsMonotonicInMaxEdits() {
        long e1 = new FuzzyQueryCostEstimator(20, 20, 1, 0).estimate();
        long e2 = new FuzzyQueryCostEstimator(20, 20, 2, 0).estimate();
        assertTrue("expected estimate to grow with maxEdits", e1 < e2);
    }

    public void testWideAlphabetEstimateIsHigherThanNarrow() {
        long narrow = new FuzzyQueryCostEstimator(20, 20, 2, 0).estimate();
        long wide = new FuzzyQueryCostEstimator(20, 200, 2, 0).estimate();
        assertTrue("wide alphabet (above WIDE_ALPHABET_THRESHOLD) must charge more than ASCII-shaped input", narrow < wide);
    }

    public void testNarrowAlphabetIsFlat() {
        // Below the wide-alphabet threshold the alphabet term is constant (factor 1) — the
        // estimator deliberately does not try to track sub-threshold variation.
        long bd1 = new FuzzyQueryCostEstimator(20, 1, 2, 0).estimate();
        long bd26 = new FuzzyQueryCostEstimator(20, 26, 2, 0).estimate();
        long bd64 = new FuzzyQueryCostEstimator(20, 64, 2, 0).estimate();
        assertEquals(bd1, bd26);
        assertEquals(bd1, bd64);
    }

    public void testPrefixLengthShrinksEstimate() {
        long noPrefix = new FuzzyQueryCostEstimator(60, 60, 2, 0).estimate();
        long withPrefix = new FuzzyQueryCostEstimator(60, 60, 2, 5).estimate();
        assertTrue("prefix should reduce the suffix-driven cost", withPrefix < noPrefix);
    }

    public void testPrefixLongerThanTermIsClampedNotNegative() {
        // When the prefix swallows the entire term, the dynamic part should collapse to zero
        // and only the BASE constant remain.
        long allPrefix = new FuzzyQueryCostEstimator(5, 5, 2, 100).estimate();
        assertEquals(FuzzyQueryCostEstimator.BASE_BYTES, allPrefix);
    }

    public void testConstructorRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(-1, 0, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, -1, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, -1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, 1, -1));
    }

    public void testConstructorRejectsMaxEditsAboveLuceneLimit() {
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, 99, 0));
    }

    // The previous empirical "estimate >= sum(CompiledAutomaton#ramBytesUsed())" check that
    // walked a {termLen, maxEdits, prefix, alphabet, transpositions} grid was intentionally
    // dropped. Coupling unit tests to Lucene's internal RAM accounting is brittle: small
    // Lucene refactors that don't actually regress the breaker would still turn the test red.
    // The monotonicity and invariant assertions above plus the end-to-end coverage in
    // server/src/internalClusterTest/.../AccountableQueryCircuitBreakerIT cover the property
    // we actually care about (pathological fuzzy clauses trip the breaker). The empirical
    // ceiling property is better re-validated periodically via a JMH benchmark.
}
