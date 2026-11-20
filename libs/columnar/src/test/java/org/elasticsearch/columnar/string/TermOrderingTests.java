/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntBinaryOperator;

public class TermOrderingTests extends ESTestCase {

    public void testByTermIsTermOrder() {
        final Fixture fixture = fixture("WARN", "DEBUG", "", "TRACE", "INFO");
        assertEquals(List.of("", "DEBUG", "INFO", "TRACE", "WARN"), fixture.byTerm());
    }

    public void testARuleDecidesAndTheTermBreaksTies() {
        final Fixture fixture = fixture("WARN", "DEBUG", "TRACE", "INFO");
        final int[] weight = { 1, 2, 2, 1 };
        assertEquals(List.of("DEBUG", "TRACE", "INFO", "WARN"), fixture.sortedBy((a, b) -> Integer.compare(weight[b], weight[a])));
    }

    public void testATermNeverDecidesWhereTheRuleAlreadyHas() {
        final Fixture fixture = fixture("aaa", "zzz");
        final int[] weight = { 1, 9 };
        assertEquals(List.of("zzz", "aaa"), fixture.sortedBy((a, b) -> Integer.compare(weight[b], weight[a])));
    }

    // NOTE: the ranking has to be total, or the same column could yield different dictionaries between runs.
    public void testAnyColumnIsOrderedTotallyAndKeepsEveryId() {
        final Set<String> distinct = new HashSet<>();
        for (int term = 0; term < between(1, 200); term++) {
            distinct.add(randomAlphaOfLengthBetween(0, 8));
        }
        final Fixture fixture = fixture(distinct.toArray(String[]::new));
        final int[] weight = new int[distinct.size()];
        for (int id = 0; id < weight.length; id++) {
            weight[id] = between(1, 4);
        }

        final List<String> once = fixture.sortedBy((a, b) -> Integer.compare(weight[b], weight[a]));
        assertEquals("every id survives", distinct.size(), once.size());
        assertEquals("and none is duplicated", distinct, new HashSet<>(once));
        assertEquals("the same input orders the same way twice", once, fixture.sortedBy((a, b) -> Integer.compare(weight[b], weight[a])));

        final List<String> byTerm = fixture.byTerm();
        final List<String> expected = new ArrayList<>(distinct);
        expected.sort(null);
        assertEquals(expected, byTerm);
    }

    private record Fixture(BytesRefHash terms, int[] ids, TermOrdering ordering) {

        List<String> byTerm() {
            final int[] copy = Arrays.copyOf(ids, ids.length);
            ordering.byTerm(copy, 0, copy.length);
            return termsOf(copy);
        }

        List<String> sortedBy(IntBinaryOperator rule) {
            final int[] copy = Arrays.copyOf(ids, ids.length);
            ordering.sort(copy, 0, copy.length, rule);
            return termsOf(copy);
        }

        private List<String> termsOf(int[] ordered) {
            final BytesRef scratch = new BytesRef();
            final List<String> out = new ArrayList<>(ordered.length);
            for (int id : ordered) {
                terms.get(id, scratch);
                out.add(scratch.utf8ToString());
            }
            return out;
        }
    }

    private static Fixture fixture(String... values) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        for (String value : values) {
            terms.add(new BytesRef(value));
        }
        final int[] ids = new int[terms.size()];
        for (int id = 0; id < ids.length; id++) {
            ids[id] = id;
        }
        return new Fixture(terms, ids, new TermOrdering(terms));
    }
}
