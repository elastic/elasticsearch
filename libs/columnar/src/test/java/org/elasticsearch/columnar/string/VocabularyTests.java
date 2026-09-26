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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The survey that decides which terms a dictionary would hold. It is an approximation under a memory
 * bound, so what is asserted here are the properties it promises rather than a particular vocabulary:
 * counts never overstate, the terms most of the column holds survive however late they are first seen,
 * and the same column always yields the same dictionary.
 */
public class VocabularyTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    // NOTE: the summary is bounded by its own policy, not by the dictionary's share of the column.
    public void testSummaryIsBoundedByItsOwnCap() throws IOException {
        final List<BytesRef> values = List.of(new BytesRef("alpha"), new BytesRef("bravo"), new BytesRef("charlie"));
        final Vocabulary.Terms surveyed = survey(values, ROOMY, new SummaryPolicy(5));
        assertNotNull(surveyed);
        assertEquals("one five byte term fits", 1, surveyed.summarySize());
    }

    // NOTE: a merged column can hold one term more often than an int counts, and the summary carries the
    // counts as vlongs, so they are counted as longs throughout. Narrowing them would order the two largest
    // terms by term rather than by how often they are held, and understate what the next merge reads.
    public void testCombinedRanksOnCountsPastWhatAnIntHolds() {
        final long smaller = Integer.MAX_VALUE + 1L;
        final long larger = 2 * smaller;
        final Vocabulary.Terms combined = Vocabulary.combined(
            Map.of(new BytesRef("a"), smaller, new BytesRef("b"), larger),
            smaller + larger,
            smaller + larger,
            new DictionaryPolicy(1, 0.5, 1.0),
            new SummaryPolicy(1)
        );
        assertNotNull(combined);
        assertEquals("room for one term", 1, combined.size());
        final BytesRef kept = new BytesRef();
        combined.terms().get(combined.dictionaryIds()[0], kept);
        assertEquals("the term held more often takes the budget", "b", kept.utf8ToString());
        assertEquals("and the count it carries for the next merge is the one it was given", larger, combined.countOf(0));
    }

    public void testASummaryAllowedMoreThanTheDictionaryWidensTheTable() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int term = 0; term < 200; term++) {
            final BytesRef value = new BytesRef("term-" + term);
            values.add(value);
            values.add(value);
        }
        final DictionaryPolicy narrow = new DictionaryPolicy(32, 0.5, 1.0);
        assertThat(
            "the dictionary's cap alone bounds what the survey holds",
            survey(values, narrow, new SummaryPolicy(32)).summarySize(),
            lessThan(20)
        );
        assertEquals(
            "every term, once the summary is allowed to ask for them",
            200,
            survey(values, narrow, new SummaryPolicy(8 * 1024)).summarySize()
        );
    }

    public void testCoverageIsAShareOfValues() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add(new BytesRef("INFO"));
            values.add(new BytesRef("an-identifier-held-once-" + i));
        }
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(List.of("INFO"), termsOf(surveyed));
        assertEquals("half the values, far less than half the bytes", 0.5, surveyed.coverage(), 1e-9);
    }

    // NOTE: the denominator is every value the column held, not what the table still holds. Deriving it from
    // the table would count only the survivors of an eviction and overstate what the dictionary reaches.
    public void testEvictedOccurrencesStayInTheDenominator() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(new BytesRef("h"));
        }
        values.add(new BytesRef("aa"));
        values.add(new BytesRef("bb"));
        values.add(new BytesRef("cc"));

        final Vocabulary.Terms surveyed = survey(values, new DictionaryPolicy(5, 0.5, 1.0), new SummaryPolicy(5));
        assertNotNull(surveyed);
        long named = 0;
        for (int ordinal = 0; ordinal < surveyed.size(); ordinal++) {
            named += surveyed.countOf(ordinal);
        }
        assertEquals("a share of all 103 values the column held", (double) named / 103, surveyed.coverage(), 1e-9);
    }

    /** A term seen many times is kept, however late in the column it first appears. */
    public void testKeepsWhatTheColumnRepeats() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            // A long tail first, so the common terms all arrive after the table has had to make room.
            values.add(new BytesRef("tail-" + i));
        }
        final List<String> common = List.of("INFO", "DEBUG", "WARN", "ERROR");
        for (int i = 0; i < 8000; i++) {
            values.add(new BytesRef(common.get(i % common.size())));
        }
        final Vocabulary.Terms surveyed = survey(values, new DictionaryPolicy(4096, 0.5, 0.2));
        assertNotNull("a column this repetitive has a vocabulary", surveyed);
        for (String term : common) {
            assertTrue("kept " + term, termsOf(surveyed).contains(term));
        }
    }

    /** Counts are lower bounds: what the survey reports is never more than the column really holds. */
    public void testCountsNeverOverstate() throws IOException {
        final List<BytesRef> values = zipfish();
        final Map<String, Integer> actual = tally(values);
        final Vocabulary.Terms surveyed = survey(values, new DictionaryPolicy(2048, 0.5, 0.2));
        assertNotNull(surveyed);
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < surveyed.size(); ordinal++) {
            surveyed.terms().get(surveyed.dictionaryIds()[ordinal], term);
            final String text = term.utf8ToString();
            assertThat("count of " + text, surveyed.countOf(ordinal), lessThanOrEqualTo(actual.get(text).longValue()));
        }
    }

    /** The kept ids are in term order, so an ordinal comparison is a term comparison. */
    public void testTermsAreInTermOrder() throws IOException {
        final Vocabulary.Terms surveyed = survey(zipfish(), ROOMY);
        assertNotNull(surveyed);
        final List<String> terms = termsOf(surveyed);
        for (int i = 1; i < terms.size(); i++) {
            assertTrue(terms.get(i - 1) + " < " + terms.get(i), terms.get(i - 1).compareTo(terms.get(i)) < 0);
        }
    }

    /**
     * A term seen once buys one value of coverage and would widen the ordinal every value in the column
     * pays for, so a column of nothing but distinct values has no vocabulary at all.
     */
    // NOTE: nothing here repays an ordinal in this column, but a term held once per segment is held once per
    // segment in every segment, so the merge has to be told. Refusing a vocabulary would send it to the values.
    public void testAllDistinctValuesStillSummarise() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add(new BytesRef("id-" + i));
        }
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals("no term repays an ordinal", 0, surveyed.size());
        assertEquals("every term is summarised", 2000, surveyed.summarySize());
        assertFalse(
            "so the column is written plain",
            ROOMY.worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes())
        );
    }

    /** The same column always yields the same dictionary, so a segment does not depend on how it was read. */
    public void testSameColumnYieldsTheSameDictionary() throws IOException {
        final List<BytesRef> values = zipfish();
        assertEquals(termsOf(survey(values, ROOMY)), termsOf(survey(values, ROOMY)));
    }

    /** The dictionary stays inside the budget the policy allows for a column of this size. */
    public void testStaysWithinItsBudget() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(1024, 0.5, 0.2);
        final Vocabulary.Terms surveyed = survey(zipfish(), policy);
        assertNotNull(surveyed);
        assertThat(
            "dictionary bytes within budget",
            surveyed.dictionaryBytes(),
            lessThanOrEqualTo(policy.budgetFor(surveyed.columnBytes()))
        );
    }

    /**
     * A vocabulary taken from what other columns recorded, rather than surveyed from values. It has to keep
     * the terms in the order it was given, name each one by where it sits, and carry the counts when it was
     * given any.
     */
    public void testKnownTermsAreOrdinalsInTermOrder() {
        final List<BytesRef> sorted = List.of(new BytesRef(""), new BytesRef("alpha"), new BytesRef("bravo"), new BytesRef("charlie"));
        final long[] counts = { 9, 4, 7, 1 };
        final Vocabulary.Terms known = Vocabulary.known(sorted, 1000, 0.75, counts);

        assertEquals("one ordinal a term", sorted.size(), known.size());
        assertTrue("counts were given", known.counted());
        assertEquals("coverage is kept as given", 0.75, known.coverage(), 0.0);
        assertEquals("the terms' own bytes, the empty one charged the byte it costs", 1 + 5 + 5 + 7, known.dictionaryBytes());
        assertEquals("column bytes are kept as given", 1000, known.columnBytes());

        final BytesRef scratch = new BytesRef();
        for (int ordinal = 0; ordinal < sorted.size(); ordinal++) {
            known.terms().get(known.dictionaryIds()[ordinal], scratch);
            assertEquals("term at ordinal " + ordinal, sorted.get(ordinal), scratch);
            assertEquals("ordinal round trips through its id", ordinal, known.ordinalOfId()[known.dictionaryIds()[ordinal]]);
            assertEquals("count at ordinal " + ordinal, counts[ordinal], known.countOf(ordinal));
        }
    }

    /** Given no counts, the vocabulary says so rather than inventing them. */
    public void testKnownTermsWithoutCounts() {
        final List<BytesRef> sorted = List.of(new BytesRef("alpha"), new BytesRef("bravo"));
        final Vocabulary.Terms known = Vocabulary.known(sorted, 500, 1.0, null);
        assertFalse("no counts were given", known.counted());
        assertEquals(sorted.size(), known.size());
        assertEquals("coverage is kept as given", 1.0, known.coverage(), 0.0);
    }

    public void testCoverageCountsValuesWhateverTheyWeigh() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        final String[] frequent = { "alpha", "bravo", "char.", "delta", "echo." };
        for (int i = 0; i < 1000; i++) {
            values.add(new BytesRef(frequent[i % frequent.length]));
        }
        values.addAll(singletons(1000, 50));
        // NOTE: named values are 5 bytes and escapes 50, so this column is 9% covered by weight, half by count.
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(5, surveyed.size());
        assertEquals(0.5, surveyed.coverage(), 1e-9);
        assertFalse(
            "half the reads still escape",
            new DictionaryPolicy(512 * 1024, 0.9, 0.2).worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes())
        );
    }

    public void testCoverageNeverOverstates() throws IOException {
        final List<BytesRef> values = zipfish();
        final Map<String, Integer> actual = tally(values);
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        long trulyCovered = 0;
        for (String term : termsOf(surveyed)) {
            trulyCovered += actual.get(term);
        }
        assertThat("coverage", surveyed.coverage(), lessThanOrEqualTo((double) trulyCovered / values.size() + 1e-9));
    }

    public void testLongNamedValuesNameNoMoreOfTheColumn() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        final byte[] term = new byte[50];
        for (int t = 0; t < 5; t++) {
            term[0] = (byte) t;
            final BytesRef ref = new BytesRef(term.clone());
            for (int i = 0; i < 200; i++) {
                values.add(ref);
            }
        }
        values.addAll(singletons(1000, 5));
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(5, surveyed.size());
        assertEquals("a thousand values named of two thousand, as when they were short", 0.5, surveyed.coverage(), 1e-9);
        assertTrue(ROOMY.worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes()));
    }

    /** All values in the dictionary: coverage is 1.0 and the policy accepts trivially. */
    public void testCoverageIsOneWhenAllValuesCovered() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        final String[] terms = { "alpha", "bravo", "char.", "delta", "echo." };
        for (int i = 0; i < 2000; i++) {
            values.add(new BytesRef(terms[i % terms.length]));
        }
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(5, surveyed.size());
        assertEquals("every byte is covered", 1.0, surveyed.coverage(), 1e-9);
        assertTrue(ROOMY.worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes()));
    }

    public void testEmptyStringColumnAcceptsDictionary() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add(new BytesRef(""));
        }
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(1, surveyed.size());
        assertEquals("all values are the covered empty string", 1.0, surveyed.coverage(), 1e-9);
        assertEquals("the byte the selection was charged for it", 1, surveyed.dictionaryBytes());
        assertTrue(ROOMY.worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes()));
    }

    public void testEmptyStringCoverageWithLongEscapes() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(new BytesRef(""));
        }
        values.addAll(singletons(1000, 50));
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        assertEquals(1, surveyed.size());
        assertEquals(0.5, surveyed.coverage(), 1e-9);
        assertFalse(
            "half the reads still escape",
            new DictionaryPolicy(512 * 1024, 0.9, 0.2).worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes())
        );
    }

    /**
     * A value longer than the whole byte bound cannot be admitted, and with an empty table there is nothing
     * to evict to make room for it. The survey has to decline it rather than try.
     */
    public void testValueLongerThanTheWholeBound() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            values.add(new BytesRef(randomAlphaOfLength(200)));
        }
        // Every value is longer than the bound, so the table never holds anything.
        assertNull("nothing fits", survey(values, new DictionaryPolicy(16, 0.5, 0.2), new SummaryPolicy(16)));
    }

    /** The first value alone exceeds the bound, and the terms after it still have to be surveyed. */
    public void testOversizedFirstValueDoesNotStopTheSurvey() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        values.add(new BytesRef(randomAlphaOfLength(500)));
        for (int i = 0; i < 400; i++) {
            values.add(new BytesRef(i % 2 == 0 ? "on" : "off"));
        }
        final Vocabulary.Terms surveyed = survey(values, new DictionaryPolicy(64, 0.5, 0.2), new SummaryPolicy(64));
        assertNotNull("the short terms were still found", surveyed);
        assertTrue("kept what repeats", termsOf(surveyed).contains("on"));
        assertTrue("kept what repeats", termsOf(surveyed).contains("off"));
    }

    /** A handful of terms holding most of the column, over a long tail of terms seen once. */
    private List<BytesRef> zipfish() {
        final List<BytesRef> values = new ArrayList<>();
        final String[] head = { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };
        for (int i = 0; i < 6000; i++) {
            values.add(new BytesRef(head[i % head.length]));
        }
        for (int i = 0; i < 1500; i++) {
            values.add(new BytesRef("rare-" + i));
        }
        java.util.Collections.shuffle(values, random());
        return values;
    }

    private static Map<String, Integer> tally(List<BytesRef> values) {
        final Map<String, Integer> counts = new HashMap<>();
        for (BytesRef value : values) {
            counts.merge(value.utf8ToString(), 1, Integer::sum);
        }
        return counts;
    }

    private static List<BytesRef> singletons(int count, int length) {
        final List<BytesRef> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final byte[] value = new byte[length];
            value[0] = (byte) (i & 0xff);
            value[1] = (byte) ((i >> 8) & 0xff);
            values.add(new BytesRef(value));
        }
        return values;
    }

    private static List<String> termsOf(Vocabulary.Terms surveyed) {
        final List<String> terms = new ArrayList<>();
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < surveyed.size(); ordinal++) {
            surveyed.terms().get(surveyed.dictionaryIds()[ordinal], term);
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    private static Vocabulary.Terms survey(List<BytesRef> values, DictionaryPolicy policy) throws IOException {
        return survey(values, policy, StringColumnOptions.DEFAULT_SUMMARY);
    }

    private static Vocabulary.Terms survey(List<BytesRef> values, DictionaryPolicy policy, SummaryPolicy summaryPolicy) throws IOException {
        return Vocabulary.survey(cursor(values.toArray(BytesRef[]::new)), policy, summaryPolicy);
    }
}
