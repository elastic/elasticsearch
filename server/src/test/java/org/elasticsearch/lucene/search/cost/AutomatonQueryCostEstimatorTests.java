/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

public class AutomatonQueryCostEstimatorTests extends ESTestCase {

    public void testConstructorRejectsNegativeDfaRamBytes() {
        expectThrows(IllegalArgumentException.class, () -> new AutomatonQueryCostEstimator(-1));
    }

    public void testEstimateScalesWithDfa() {
        // Pick inputs that comfortably exceed the floor so we exercise the multiplier path,
        // not the floor.
        long mediumDfa = AutomatonQueryCostEstimator.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES;
        long largeDfa = mediumDfa * 100;
        assertEquals(
            mediumDfa * AutomatonQueryCostEstimator.COMPILED_AUTOMATON_PEAK_MULTIPLIER,
            new AutomatonQueryCostEstimator(mediumDfa).estimate()
        );
        assertEquals(
            largeDfa * AutomatonQueryCostEstimator.COMPILED_AUTOMATON_PEAK_MULTIPLIER,
            new AutomatonQueryCostEstimator(largeDfa).estimate()
        );
    }

    public void testEstimateAppliesFloorForSmallDfa() {
        // A tiny DFA's K-multiplied size is below the floor; the floor must dominate.
        long tinyDfa = 100L;
        long reservation = new AutomatonQueryCostEstimator(tinyDfa).estimate();
        assertEquals(AutomatonQueryCostEstimator.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES, reservation);
        assertTrue(
            "reservation must not regress below the floor",
            reservation >= AutomatonQueryCostEstimator.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES
        );
    }

    public void testEstimateIsZeroSafe() {
        // Defensive: a 0-byte DFA shouldn't yield a 0 reservation; the floor still applies.
        assertEquals(AutomatonQueryCostEstimator.COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES, new AutomatonQueryCostEstimator(0).estimate());
    }

    /**
     * Verifies that the pre-flight CB reservation covers the post-construction query size for each
     * known pattern family. {@code reservation / actual} ratios are logged so they can be used to
     * tune {@link AutomatonQueryCostEstimator#COMPILED_AUTOMATON_PEAK_MULTIPLIER} and
     * {@link AutomatonQueryCostEstimator#COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES} over time.
     * <p>
     * Note: {@code actual} is the final retained size from {@code query.ramBytesUsed()}, not the
     * construction peak. The reservation must cover the peak; this test only verifies the final
     * state as a lower bound.
     */
    public void testEstimateCoversActualSize() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test");
        String[][] patterns = {
            { "simple_ascii", "foo*bar" },
            { "many_wildcards", "a*b?c*d?e*f?g*h" },
            { "multibyte_utf8", "日".repeat(60) + "*" },
            { "adversarial_question", "?".repeat(1000) },
            { "long_literal", "a".repeat(50) + "*" + "b".repeat(50) }, };
        for (String[] entry : patterns) {
            String name = entry[0];
            String pattern = entry[1];
            Term term = new Term("field", pattern);
            Automaton dfa = AutomatonQueries.toWildcardAutomaton(term, breaker);
            long reservation = new AutomatonQueryCostEstimator(dfa.ramBytesUsed()).estimate();
            long actual = new WildcardQuery(term).ramBytesUsed();
            logger.info(
                "CB reservation ratio [{}]: dfa={} B  reservation={} B  actual={} B  ratio={}",
                name,
                dfa.ramBytesUsed(),
                reservation,
                actual,
                String.format(Locale.ROOT, "%.1f", (double) reservation / actual)
            );
            assertTrue("reservation must cover actual size for pattern [" + name + "]", reservation >= actual);
        }
    }

    public void testEstimateSaturatesOnOverflow() {
        // A DFA so large that ramBytes * multiplier would overflow long arithmetic must saturate
        // to Long.MAX_VALUE rather than wrap to a small positive (or negative) value that would
        // silently under-reserve.
        long overflowingDfa = Long.MAX_VALUE / AutomatonQueryCostEstimator.COMPILED_AUTOMATON_PEAK_MULTIPLIER + 1;
        assertEquals(Long.MAX_VALUE, new AutomatonQueryCostEstimator(overflowingDfa).estimate());
        assertEquals(Long.MAX_VALUE, new AutomatonQueryCostEstimator(Long.MAX_VALUE).estimate());
    }
}
