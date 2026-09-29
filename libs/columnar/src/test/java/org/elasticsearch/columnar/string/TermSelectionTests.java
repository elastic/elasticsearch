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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TermSelectionTests extends ESTestCase {

    private static final DictionaryPolicy ROOMY_DICTIONARY = new DictionaryPolicy(512 * 1024, 0.5, 1.0);
    private static final SummaryPolicy ROOMY_SUMMARY = new SummaryPolicy(512 * 1024);

    public void testDictionaryLeavesOutTermsHeldOnceAndASummaryKeepsThem() {
        final Fixture fixture = fixture(Map.of("INFO", 10, "WARN", 5, "DEBUG", 1));
        assertEquals(List.of("INFO", "WARN"), fixture.forDictionary(ROOMY_DICTIONARY, 1000));
        assertEquals(List.of("DEBUG", "INFO", "WARN"), fixture.forSummary(ROOMY_SUMMARY));
    }

    // NOTE: the share bounds a dictionary against the column it describes, which is a question about this column. A summary
    // answers to the merge that reads it, so only the absolute cap bounds it.
    public void testTheShareOfTheColumnBoundsOnlyTheDictionary() {
        final Fixture fixture = fixture(Map.of("INFO", 10, "WARN", 5, "ERROR", 3));
        // Eight bytes of the forty this column holds, which is room for the two four-byte terms and not the five-byte one.
        final DictionaryPolicy fifth = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
        assertEquals(List.of("INFO", "WARN"), fixture.forDictionary(fifth, 40));
        assertEquals(List.of("ERROR", "INFO", "WARN"), fixture.forSummary(ROOMY_SUMMARY));
    }

    public void testTheCapBoundsASummary() {
        final Fixture fixture = fixture(Map.of("INFO", 10, "WARN", 5, "ERROR", 3));
        assertEquals(List.of("INFO", "WARN"), fixture.forSummary(new SummaryPolicy(8)));
        assertEquals(List.of(), fixture.forSummary(SummaryPolicy.NONE));
    }

    // NOTE: terms of equal standing are ordered by term, so a column yields the same answer however its values arrived.
    public void testTermsHeldEquallyOftenAreOrderedByTerm() {
        final Fixture fixture = fixture(Map.of("TRACE", 5, "DEBUG", 5, "ERROR", 5));
        assertEquals(List.of("DEBUG"), fixture.forDictionary(new DictionaryPolicy(5, 0.5, 1.0), 1000));
        assertEquals(List.of("DEBUG", "ERROR"), fixture.forDictionary(new DictionaryPolicy(10, 0.5, 1.0), 1000));
    }

    // NOTE: the empty term occupies no bytes but does occupy an entry, so a budget of nothing buys it no more
    // than it buys any other term. Charging its true length would let it past every bound there is.
    public void testTheEmptyTermDoesNotFitABudgetOfNothing() {
        final Fixture fixture = fixture(Map.of("", 9, "INFO", 4));
        assertEquals(List.of(), fixture.forSummary(SummaryPolicy.NONE));
        assertEquals(List.of(""), fixture.forSummary(new SummaryPolicy(1)));
    }

    // NOTE: whatever the column holds, an answer stays inside the quota, admits nothing rarer than the quota
    // allows, and only grows as the budget does, since every answer is a prefix of one ranking.
    public void testAnyColumnIsAnsweredWithinItsQuota() {
        final Map<String, Integer> termCounts = new LinkedHashMap<>();
        for (int term = 0; term < between(1, 60); term++) {
            termCounts.put(randomAlphaOfLengthBetween(0, 12), between(1, 1000));
        }
        final Fixture fixture = fixture(termCounts);

        final int cap = between(0, 200);
        final List<String> summarised = fixture.forSummary(new SummaryPolicy(cap));
        assertEquals("in term order", summarised.stream().sorted().toList(), summarised);
        assertThat(
            "within the cap",
            summarised.stream().mapToLong(term -> Math.max(1, term.length())).sum(),
            lessThanOrEqualTo((long) cap)
        );
        assertTrue(
            "a larger cap keeps what a smaller one kept",
            fixture.forSummary(new SummaryPolicy(cap + between(1, 200))).containsAll(summarised)
        );

        final List<String> named = fixture.forDictionary(ROOMY_DICTIONARY, 1000);
        assertEquals("in term order", named.stream().sorted().toList(), named);
        for (String term : named) {
            assertThat("no term held once earns a dictionary entry", termCounts.get(term), greaterThanOrEqualTo(2));
        }
        assertTrue("a summary holds what the dictionary does, and more", fixture.forSummary(ROOMY_SUMMARY).containsAll(named));
    }

    public void testWhatIsKeptComesBackInTermOrder() {
        final Fixture fixture = fixture(Map.of("TRACE", 2, "INFO", 9, "WARN", 4));
        assertEquals(List.of("INFO", "TRACE", "WARN"), fixture.forDictionary(ROOMY_DICTIONARY, 1000));
    }

    /** A surveyed column: its terms, how often each was seen, and the selection that reads them. */
    private record Fixture(BytesRefHash terms, TermSelection selection) {

        List<String> forDictionary(DictionaryPolicy dictionaryPolicy, long columnBytes) {
            return termsOf(selection.thatFit(TermQuota.forDictionary(dictionaryPolicy, columnBytes)));
        }

        List<String> forSummary(SummaryPolicy summaryPolicy) {
            return termsOf(selection.thatFit(TermQuota.forSummary(summaryPolicy)));
        }

        private List<String> termsOf(int[] ids) {
            final BytesRef scratch = new BytesRef();
            final List<String> kept = new ArrayList<>(ids.length);
            for (int id : ids) {
                terms.get(id, scratch);
                kept.add(scratch.utf8ToString());
            }
            return kept;
        }
    }

    private static Fixture fixture(Map<String, Integer> termCounts) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        final Map<String, Integer> ordered = new LinkedHashMap<>(termCounts);
        final int[] counts = new int[ordered.size()];
        for (Map.Entry<String, Integer> entry : ordered.entrySet()) {
            int id = terms.add(new BytesRef(entry.getKey()));
            if (id < 0) {
                id = -1 - id;
            }
            counts[id] = entry.getValue();
        }
        return new Fixture(terms, new TermSelection(terms, counts));
    }
}
