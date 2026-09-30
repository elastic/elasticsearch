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

import static org.hamcrest.Matchers.greaterThan;
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
    public void testTermsOfEqualDensityAreOrderedByTerm() {
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

    // NOTE: a term the budget cannot afford is stepped over rather than ending the walk, so an answer packs
    // the density ranking rather than prefixing it. Budgets therefore choose sets that need not nest in
    // either direction: eleven bytes spent on one term leave less room than six bytes never spent on it.
    public void testSelectionsUnderDifferentBudgetsNeedNotNest() {
        final Map<String, Integer> termCounts = new LinkedHashMap<>();
        termCounts.put("aaaaaaaaaa", 100);
        termCounts.put("bbb", 12);
        termCounts.put("ccc", 12);
        final Fixture fixture = fixture(termCounts);

        assertEquals("ten bytes buys nothing, so six bytes buys both", List.of("bbb", "ccc"), fixture.forSummary(new SummaryPolicy(6)));
        assertEquals(
            "eleven bytes buys the densest term and no room after it",
            List.of("aaaaaaaaaa"),
            fixture.forSummary(new SummaryPolicy(11))
        );
    }

    // NOTE: a larger budget can keep a different set of terms, but never names fewer values. At the first
    // term two budgets decide differently, the one the larger affords outranks everything behind it by
    // density and costs more than the smaller has left, so what the smaller packs into that capacity names
    // at most its density times that capacity, which is less than the skipped term names alone. The bound
    // is on a sum over any subset, so indivisible terms do not break it.
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
        final long spent = summarised.stream().mapToLong(term -> Math.max(1, term.length())).sum();
        for (String term : termCounts.keySet()) {
            if (summarised.contains(term) == false) {
                assertThat("no room was left for [" + term + "]", spent + Math.max(1, term.length()), greaterThan((long) cap));
            }
        }
        final List<String> wider = fixture.forSummary(new SummaryPolicy(cap + between(1, 200)));
        assertThat(
            "a larger cap names at least as many values",
            wider.stream().mapToLong(termCounts::get).sum(),
            greaterThanOrEqualTo(summarised.stream().mapToLong(termCounts::get).sum())
        );

        final List<String> named = fixture.forDictionary(ROOMY_DICTIONARY, 1000);
        assertEquals("in term order", named.stream().sorted().toList(), named);
        for (String term : named) {
            assertThat("no term held once earns a dictionary entry", termCounts.get(term), greaterThanOrEqualTo(2));
        }
        assertTrue("a summary holds what the dictionary does, and more", fixture.forSummary(ROOMY_SUMMARY).containsAll(named));
    }

    public void testAShortTermOutranksALongerOneHeldMoreOften() {
        final Fixture fixture = fixture(Map.of("WARN", 100, "deprecation.warning.repeated", 500));
        // Room for one of the two. Ranked by count the long term takes it and does not fit; ranked by what each names per byte
        // the short one wins, naming a value every four bytes against the long term's twenty-eight.
        assertEquals(List.of("WARN"), fixture.forDictionary(new DictionaryPolicy(27, 0.5, 1.0), 10_000));
    }

    public void testTermsHeldEquallyOftenAreOrderedByLength() {
        final Fixture fixture = fixture(Map.of("aaaaaaaa", 10, "zz", 10));
        // NOTE: equal counts, so a ranking by count alone would tie and fall to term order, taking the eight
        // byte term first and fitting neither. By what each names per byte the two byte term is four times
        // the better buy.
        assertEquals(List.of("zz"), fixture.forDictionary(new DictionaryPolicy(2, 0.5, 1.0), 10_000));
        assertEquals(List.of("aaaaaaaa", "zz"), fixture.forDictionary(new DictionaryPolicy(10, 0.5, 1.0), 10_000));
    }

    public void testATermTooLargeForTheBudgetDoesNotEndTheWalk() {
        // NOTE: by density the ten byte term held a thousand times leads the four byte term held two
        // hundred, and a budget of eight pays for neither of them together nor the leader alone. Stopping
        // at the first term that does not fit would leave the column no dictionary at all.
        final Fixture fixture = fixture(Map.of("t".repeat(10), 1000, "s".repeat(4), 200));
        assertEquals(List.of("s".repeat(4)), fixture.forDictionary(new DictionaryPolicy(8, 0.5, 1.0), 10_000));
    }

    public void testATermTheQuotaRefusesDoesNotEndTheWalk() {
        // NOTE: by density the single byte term held once leads the two hundred byte term held a hundred
        // times, and the dictionary admits no term held once. Stopping at the first term it refuses would
        // cost it the one behind, which names a hundred of the hundred and one values.
        final Fixture fixture = fixture(Map.of("a", 1, "t".repeat(200), 100));
        assertEquals(List.of("t".repeat(200)), fixture.forDictionary(new DictionaryPolicy(512, 0.5, 1.0), 10_000));
        assertEquals(List.of("a", "t".repeat(200)), fixture.forSummary(new SummaryPolicy(512)));
    }

    // NOTE: the two quotas admit different terms, so the same size does not make the same set.
    public void testTheSameSizeDoesNotMakeASummaryTheDictionary() {
        final Fixture fixture = fixture(Map.of("a", 1, "t".repeat(200), 100));
        assertEquals(List.of("t".repeat(200)), fixture.forDictionary(new DictionaryPolicy(200, 0.5, 1.0), 10_000));
        assertEquals("one term each, and not the same one", List.of("a"), fixture.forSummary(new SummaryPolicy(200)));
    }

    public void testDensitiesAreComparedAsFractions() {
        // NOTE: three halves against four thirds. Integer division would make both one and let term order
        // decide, which takes the wrong term first.
        final Fixture fixture = fixture(Map.of("zz", 3, "aaa", 4));
        assertEquals(List.of("zz"), fixture.forDictionary(new DictionaryPolicy(2, 0.5, 1.0), 10_000));
    }

    public void testTrulyEqualDensitiesFallBackToTermOrder() {
        // NOTE: five halves against ten quarters, so the counts differ and the densities do not.
        final Fixture fixture = fixture(Map.of("aa", 5, "zzzz", 10));
        assertEquals(List.of("aa"), fixture.forDictionary(new DictionaryPolicy(2, 0.5, 1.0), 10_000));
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
        final long[] counts = new long[ordered.size()];
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
