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

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The survey that decides which terms a dictionary would hold. It is an approximation under a memory
 * bound, so what is asserted here are the properties it promises rather than a particular vocabulary:
 * counts never overstate, the terms most of the column holds survive however late they are first seen,
 * and the same column always yields the same dictionary.
 */
public class VocabularyTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

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
            surveyed.terms().get(surveyed.sortedIds()[ordinal], term);
            final String text = term.utf8ToString();
            assertThat("count of " + text, surveyed.countOf(ordinal), lessThanOrEqualTo(actual.get(text)));
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
    public void testAllDistinctValuesHaveNoVocabulary() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add(new BytesRef("id-" + i));
        }
        assertNull("nothing repeats", survey(values, ROOMY));
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

    /** Coverage is what the counts say, and the counts are lower bounds, so it never overstates either. */
    public void testCoverageNeverOverstates() throws IOException {
        final List<BytesRef> values = zipfish();
        final Map<String, Integer> actual = tally(values);
        final Vocabulary.Terms surveyed = survey(values, ROOMY);
        assertNotNull(surveyed);
        long truly = 0;
        for (String term : termsOf(surveyed)) {
            truly += actual.get(term);
        }
        assertThat("coverage", surveyed.coverage(), lessThanOrEqualTo((double) truly / values.size() + 1e-9));
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
        assertNull("nothing fits", survey(values, new DictionaryPolicy(16, 0.5, 0.2)));
    }

    /** The first value alone exceeds the bound, and the terms after it still have to be surveyed. */
    public void testOversizedFirstValueDoesNotStopTheSurvey() throws IOException {
        final List<BytesRef> values = new ArrayList<>();
        values.add(new BytesRef(randomAlphaOfLength(500)));
        for (int i = 0; i < 400; i++) {
            values.add(new BytesRef(i % 2 == 0 ? "on" : "off"));
        }
        final Vocabulary.Terms surveyed = survey(values, new DictionaryPolicy(64, 0.5, 0.2));
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

    private static List<String> termsOf(Vocabulary.Terms surveyed) {
        final List<String> terms = new ArrayList<>();
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < surveyed.size(); ordinal++) {
            surveyed.terms().get(surveyed.sortedIds()[ordinal], term);
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    private static Vocabulary.Terms survey(List<BytesRef> values, DictionaryPolicy policy) throws IOException {
        return Vocabulary.survey(cursor(values.toArray(BytesRef[]::new)), policy, values.size());
    }
}
