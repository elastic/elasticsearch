/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * What a merge settles from summaries alone. Everything here is plain terms and counts: the point of the
 * component is that deciding needs no index, no codec files and no values.
 */
public class SummaryMergerTests extends ESTestCase {

    public void testStaleCountsNeitherWitnessNorRefuse() {
        final SummaryMerger witness = mergerCappedAt(4);
        witness.add(250, 700, BestCoverage.UNKNOWN, terms("dead"), counts(150L));
        assertEquals("live counts witness a dictionary", SummaryMerger.Outcome.DICTIONARY, witness.decide(true).outcome());
        assertEquals("deleted ones witness nothing", SummaryMerger.Outcome.UNDECIDED, witness.decide(false).outcome());

        final SummaryMerger refusal = mergerCappedAt(64);
        addWideVocabulary(refusal);
        assertEquals("live counts refuse one", SummaryMerger.Outcome.NO_DICTIONARY, refusal.decide(true).outcome());
        assertEquals("deleted ones refuse nothing", SummaryMerger.Outcome.UNDECIDED, refusal.decide(false).outcome());
        assertFalse("and leave no bound behind either way", refusal.decide(false).bestCoverage().known());
    }

    public void testTheBoundDoesNotRoundDownCountsPastWhatADoubleHolds() {
        final long past = (1L << 53) + 1;
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(1, Math.nextDown(1.0), 1.0), ROOMY_SUMMARY);
        merger.add(past + 1, past + 1, BestCoverage.UNKNOWN, terms("a", "b"), counts(past, 1L));

        assertThat("the one byte term fits whole", merger.decide(true).bestCoverage().namedValues(), greaterThanOrEqualTo(past));
    }

    public void testDensityProductsPastALongDoNotReverseTheOrder() {
        final long many = 1_000_000_000_000_000L;
        final Vocabulary.Terms vocabulary = Vocabulary.combined(
            Map.of(new BytesRef("a"), many, new BytesRef("b".repeat(30_000)), 1L),
            many + 30_000,
            many + 1,
            new DictionaryPolicy(1, 0.5, 1.0),
            ROOMY_SUMMARY
        );
        assertThat("the one byte term still leads", vocabulary.bestCoverage().namedValues(), greaterThanOrEqualTo(many));
    }

    private static final int PRIVATE_SEGMENTS = 40;
    private static final int PRIVATE_TERMS_PER_SEGMENT = 200;
    private static final int PRIVATE_TERM_LENGTH = 100;
    private static final int PRIVATE_REPEATS = 5;

    // NOTE: the counts are trimmed to `SummaryPolicy.mergeBudgetBytes`, so forty segments sharing no term
    // arrive with far more vocabulary than the merge keeps and most of their occurrences end up credited
    // as unaccounted mass. What each input recorded about itself escaped that trim, and is the only thing
    // here that can still refuse.
    public void testWhatEachInputRecordedRefusesWhereTheTrimmedCountsCannot() {
        assertEquals(
            "each input bounded at 410 of its 1000 values, so 16400 of 40000",
            SummaryMerger.Outcome.NO_DICTIONARY,
            decideOverPrivateVocabularies(BestCoverage.of(410, 1000, 8 * 1024)).outcome()
        );
        assertEquals(
            "the same counts, with nothing recorded, leave it to the values",
            SummaryMerger.Outcome.UNDECIDED,
            decideOverPrivateVocabularies(BestCoverage.UNKNOWN).outcome()
        );
    }

    private static SummaryMerger.Decision decideOverPrivateVocabularies(BestCoverage recorded) {
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(8 * 1024, 0.5, 0.2), SummaryPolicy.sized(64 * 1024));
        for (int segment = 0; segment < PRIVATE_SEGMENTS; segment++) {
            final List<BytesRef> terms = new ArrayList<>(PRIVATE_TERMS_PER_SEGMENT);
            final List<Long> counts = new ArrayList<>(PRIVATE_TERMS_PER_SEGMENT);
            for (int t = 0; t < PRIVATE_TERMS_PER_SEGMENT; t++) {
                terms.add(paddedTerm(segment * PRIVATE_TERMS_PER_SEGMENT + t));
                counts.add((long) PRIVATE_REPEATS);
            }
            final long numValues = (long) PRIVATE_TERMS_PER_SEGMENT * PRIVATE_REPEATS;
            merger.add(numValues, numValues * PRIVATE_TERM_LENGTH, recorded, terms, counts);
        }
        return merger.decide(true);
    }

    private static BytesRef paddedTerm(int rank) {
        return new BytesRef((rank + "-" + "x".repeat(PRIVATE_TERM_LENGTH)).substring(0, PRIVATE_TERM_LENGTH));
    }

    private static DictionaryPolicy policyWithMinCoverage(double minCoverage) {
        return new DictionaryPolicy(4, minCoverage, 1.0);
    }

    private static final SummaryPolicy ROOMY_SUMMARY = SummaryPolicy.sized(512 * 1024);

    /** Eight thirty byte terms held twelve times each: far past a sixty four byte cap, none of it trimmed. */
    private static void addWideVocabulary(SummaryMerger merger) {
        final List<BytesRef> held = new ArrayList<>();
        final List<Long> counted = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            held.add(new BytesRef("a-thirty-byte-term-number-" + (10 + t)));
            counted.add(12L);
        }
        merger.add(96, 2880, BestCoverage.UNKNOWN, held, counted);
    }

    // NOTE: past what a double holds exactly, a ratio can round below a threshold the values clear, and an
    // upper bound that refuses a feasible dictionary is not an upper bound.
    public void testABoundDoesNotRefuseOnARoundedThreshold() {
        final long named = (1L << 53) + 1;
        final long total = named + 1;
        final double threshold = Math.nextDown(1.0);
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(1, threshold, 1.0), ROOMY_SUMMARY);
        merger.add(total, total, BestCoverage.UNKNOWN, terms("a", "b"), counts(named, 1L));

        assertNotEquals(
            "a dictionary naming " + named + " of " + total + " clears the bar",
            SummaryMerger.Outcome.NO_DICTIONARY,
            merger.decide(true).outcome()
        );
    }

    public void testRulesOutComparesLargeCountsExactly() {
        for (int power = 53; power < 62; power++) {
            final long total = (1L << power) + 3;
            final String column = total + " values";
            assertFalse(column + " all named rules nothing out", policyWithMinCoverage(1.0).rulesOut(BestCoverage.of(total, total, 4)));
            assertTrue(
                column + " one short of all rules out full coverage",
                policyWithMinCoverage(1.0).rulesOut(BestCoverage.of(total - 1, total, 4))
            );
            assertTrue(
                column + " one short of all reaches the bar below",
                policyWithMinCoverage(Math.nextDown(1.0)).rulesOut(BestCoverage.of(total - 1, total, 4)) == false
            );

            final long half = total / 2 + 1;
            assertFalse(column + " over half rules nothing out", policyWithMinCoverage(0.5).rulesOut(BestCoverage.of(half, total, 4)));
            assertTrue(column + " under half rules it out", policyWithMinCoverage(0.5).rulesOut(BestCoverage.of(half - 1, total, 4)));
        }
    }

    public void testABoundNamingEveryValueRefusesNothing() {
        final long total = (1L << 53) + 3;
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(4, 1.0, 1.0), ROOMY_SUMMARY);
        merger.add(total, total, BestCoverage.of(total, total, 4), List.of(), List.of());

        assertEquals(SummaryMerger.Outcome.UNDECIDED, merger.decide(true).outcome());
    }

    // NOTE: five hundred of a thousand and one is short of half by half a value, and a bound short of the
    // bar refuses. Working the bar out in doubles and rounding it to a whole value loses that half and
    // sends a settled merge to the values.
    public void testABoundShortOfTheBarByLessThanAValueStillRefuses() {
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(1, 0.5, 1.0), ROOMY_SUMMARY);
        merger.add(1001, 1502, BestCoverage.UNKNOWN, terms("a", "bb"), counts(500L, 501L));

        assertEquals(SummaryMerger.Outcome.NO_DICTIONARY, merger.decide(true).outcome());
    }

    // NOTE: the bound a merge admitted a dictionary under can be tighter than the one the dictionary's own
    // counts give, and the merged column records what it decided on rather than recomputing a looser one.
    public void testADictionaryKeepsTheTighterBoundItWasAdmittedUnder() {
        final SummaryMerger first = mergerCappedAt(4);
        first.add(100, 100, BestCoverage.of(60, 100, 4), terms("a"), counts(50L));
        final SummaryMerger.Decision admitted = first.decide(true);

        assertEquals(SummaryMerger.Outcome.DICTIONARY, admitted.outcome());
        assertEquals("sixty, not the hundred its own counts leave open", 60, admitted.vocabulary().bestCoverage().namedValues());

        final SummaryMerger second = mergerCappedAt(4);
        second.add(100, 100, admitted.vocabulary().bestCoverage(), terms("a"), counts(50L));
        second.add(100, 100, BestCoverage.of(20, 100, 4), List.of(), List.of());

        assertEquals("eighty of two hundred cannot reach half", SummaryMerger.Outcome.NO_DICTIONARY, second.decide(true).outcome());
    }

    public void testSummedCountsThatClearTheBarProveADictionary() {
        final SummaryMerger merger = mergerCappedAt(4);
        merger.add(300, 1200, BestCoverage.UNKNOWN, terms("head"), counts(100L));
        merger.add(100, 400, BestCoverage.UNKNOWN, terms("head"), counts(100L));

        final SummaryMerger.Decision decision = merger.decide(true);
        assertEquals(SummaryMerger.Outcome.DICTIONARY, decision.outcome());
        assertEquals("head names two hundred of four hundred", 0.5, decision.vocabulary().coverage(), 1e-9);
    }

    public void testInconclusiveBoundsReturnUndecided() {
        final SummaryMerger merger = mergerCappedAt(100);
        final List<BytesRef> held = terms("head");
        final List<Long> counted = counts(80L);
        for (int i = 0; i < 120; i++) {
            held.add(new BytesRef(Integer.toString(i, 36) + "z"));
            counted.add(1L);
        }
        merger.add(200, 560, BestCoverage.UNKNOWN, held, counted);

        assertEquals(SummaryMerger.Outcome.UNDECIDED, merger.decide(true).outcome());
    }

    public void testABoundBelowTheBarRefusesAndKeepsTheTerms() {
        final SummaryMerger merger = mergerCappedAt(64);
        addWideVocabulary(merger);

        final SummaryMerger.Decision decision = merger.decide(true);
        assertEquals(SummaryMerger.Outcome.NO_DICTIONARY, decision.outcome());
        assertTrue("the bound decided it", decision.bestCoverage().known());
        assertEquals("no dictionary", 0, decision.vocabulary().dictionarySize());
        assertEquals("but every term is still recorded", 8, decision.vocabulary().summarySize());
    }

    public void testOverlappingAndDisjointTermsBothSumTheirCounts() {
        final SummaryMerger merger = mergerCappedAt(64);
        merger.add(30, 120, BestCoverage.UNKNOWN, terms("shared", "onlyA"), counts(10L, 20L));
        merger.add(40, 160, BestCoverage.UNKNOWN, terms("shared", "onlyB"), counts(5L, 35L));

        final Map<String, Long> summarised = summarisedCounts(merger.decide(true).vocabulary());
        assertEquals("the shared term sums", Long.valueOf(15), summarised.get("shared"));
        assertEquals(Long.valueOf(20), summarised.get("onlyA"));
        assertEquals(Long.valueOf(35), summarised.get("onlyB"));
    }

    public void testMissingSummaryReturnsUndecided() {
        final SummaryMerger merger = mergerCappedAt(4);
        merger.add(100, 400, BestCoverage.of(1, 100, 4), terms("head"), counts(100L));
        merger.addWithoutSummary();

        final SummaryMerger.Decision decision = merger.decide(true);
        assertEquals(SummaryMerger.Outcome.UNDECIDED, decision.outcome());
        assertFalse(decision.bestCoverage().known());
    }

    public void testABoundRecordedUnderASmallerCapCannotRefuse() {
        final SummaryMerger merger = mergerCappedAt(1024);
        final List<BytesRef> held = new ArrayList<>();
        final List<Long> counted = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            held.add(new BytesRef("id-" + i));
            counted.add(2L);
        }
        merger.add(200, 1200, BestCoverage.of(2, 200, 8), held, counted);

        assertEquals("every term repeats and fits the larger cap", SummaryMerger.Outcome.DICTIONARY, merger.decide(true).outcome());
    }

    public void testATermTwoInputsEachHeldOnceIsStillRecorded() {
        final SummaryMerger merger = new SummaryMerger(new DictionaryPolicy(512 * 1024, 0.5, 0.2), ROOMY_SUMMARY);
        merger.add(201, 803, BestCoverage.UNKNOWN, terms("head", "shared"), counts(200L, 1L));
        merger.add(201, 803, BestCoverage.UNKNOWN, terms("head", "shared"), counts(200L, 1L));

        final Map<String, Long> summarised = summarisedCounts(merger.decide(true).vocabulary());
        assertEquals("held twice by the merged column", Long.valueOf(2), summarised.get("shared"));
    }

    public void testARecordOfNoTermsStillCarriesItsValues() {
        final SummaryMerger merger = mergerCappedAt(64);
        merger.add(500, 4000, BestCoverage.of(3, 500, 64), List.of(), List.of());

        final SummaryMerger.Decision decision = merger.decide(true);
        assertEquals("the summarised bound alone refuses", SummaryMerger.Outcome.NO_DICTIONARY, decision.outcome());
        assertEquals(500, decision.bestCoverage().numValues());
    }

    public void testFrequentTermSurvivesTrimmingBeforeAndAfterNoise() {
        for (boolean headFirst : new boolean[] { true, false }) {
            final SummaryMerger merger = mergerWithEqualCaps(8);
            final List<BytesRef> noise = new ArrayList<>();
            final List<Long> noiseCounts = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                noise.add(new BytesRef("noise-" + i));
                noiseCounts.add(1L);
            }
            if (headFirst) {
                merger.add(400, 2000, BestCoverage.UNKNOWN, terms("head"), counts(400L));
                merger.add(200, 1600, BestCoverage.UNKNOWN, noise, noiseCounts);
            } else {
                merger.add(200, 1600, BestCoverage.UNKNOWN, noise, noiseCounts);
                merger.add(400, 2000, BestCoverage.UNKNOWN, terms("head"), counts(400L));
            }
            final Map<String, Long> summarised = summarisedCounts(merger.decide(true).vocabulary());
            assertEquals("head first=" + headFirst, Long.valueOf(400), summarised.get("head"));
        }
    }

    public void testTheBoundNeverUnderstatesWhatACapCouldName() {
        for (int trial = 0; trial < 200; trial++) {
            final Map<String, Long> held = new LinkedHashMap<>();
            final List<BytesRef> terms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            long numValues = 0;
            long columnBytes = 0;
            for (int t = 0; t < between(1, 8); t++) {
                final String term = randomAlphaOfLengthBetween(1, 4) + t;
                final long count = between(1, 6);
                held.put(term, count);
                terms.add(new BytesRef(term));
                counts.add(count);
                numValues += count;
                columnBytes += count * term.length();
            }
            final int cap = between(1, 24);
            final SummaryMerger merger = mergerWithEqualCaps(cap);
            merger.add(numValues, columnBytes, BestCoverage.UNKNOWN, terms, counts);
            assertThat(
                "cap=" + cap + " held=" + held,
                merger.decide(true).bestCoverage().namedValues(),
                greaterThanOrEqualTo(bestDictionary(held, cap))
            );
        }
    }

    /** The most values any set of terms costing no more than {@code cap} names, by trying every set. */
    private static long bestDictionary(Map<String, Long> held, int cap) {
        final List<Map.Entry<String, Long>> terms = new ArrayList<>(held.entrySet());
        long best = 0;
        for (int subset = 0; subset < (1 << terms.size()); subset++) {
            long bytes = 0;
            long named = 0;
            for (int t = 0; t < terms.size(); t++) {
                if ((subset & (1 << t)) != 0) {
                    bytes += Math.max(1, terms.get(t).getKey().length());
                    named += terms.get(t).getValue();
                }
            }
            if (bytes <= cap) {
                best = Math.max(best, named);
            }
        }
        return best;
    }

    private static Map<String, Long> summarisedCounts(Vocabulary.Terms vocabulary) {
        final Map<String, Long> summarised = new LinkedHashMap<>();
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < vocabulary.summarySize(); ordinal++) {
            vocabulary.terms().get(vocabulary.summaryIds()[ordinal], term);
            summarised.put(term.utf8ToString(), vocabulary.summaryCountOf(ordinal));
        }
        return summarised;
    }

    /** A merger under the shipped coverage bar, where only the dictionary byte cap varies. */
    private static SummaryMerger mergerCappedAt(int maxBytes) {
        return new SummaryMerger(new DictionaryPolicy(maxBytes, 0.5, 1.0), ROOMY_SUMMARY);
    }

    /** A merger whose summary is no wider than its dictionary, so the terms it holds are trimmed as they arrive. */
    private static SummaryMerger mergerWithEqualCaps(int maxBytes) {
        return new SummaryMerger(new DictionaryPolicy(maxBytes, 0.5, 1.0), SummaryPolicy.sized(maxBytes));
    }

    private static List<BytesRef> terms(String... values) {
        final List<BytesRef> terms = new ArrayList<>(values.length);
        for (String value : values) {
            terms.add(new BytesRef(value));
        }
        return terms;
    }

    private static List<Long> counts(Long... values) {
        return new ArrayList<>(List.of(values));
    }
}
