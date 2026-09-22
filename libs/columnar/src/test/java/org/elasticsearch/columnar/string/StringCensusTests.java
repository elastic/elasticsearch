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

/**
 * Verifies that {@link StringCensus} produces the same counts as a hand-count of the input and
 * the same vocabulary as a standalone {@link Vocabulary#survey}.
 */
public class StringCensusTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY_POLICY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /** Dense column: all maxDoc documents have one non-null value. */
    public void testDenseCounts() throws IOException {
        final BytesRef[][] docSlots = denseSlots(200, "apple", "banana", "cherry");
        assertCounts(docSlots);
    }

    /** Sparse column: roughly half the documents have a value. */
    public void testSparseCounts() throws IOException {
        final BytesRef[][] docSlots = sparseSlots(200, "alpha", "beta", "gamma");
        assertCounts(docSlots);
    }

    /** A column whose only non-empty slots are null — escapeStream and ordinals are empty. */
    public void testNullSlotsCounts() throws IOException {
        final int maxDoc = 100;
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            if (d % 3 == 0) {
                docSlots[d] = new BytesRef[] { null };
            }
        }
        assertCounts(docSlots);
    }

    /** A column spanning multiple IndexedDISI blocks (> 65536 documents). */
    public void testLargeCounts() throws IOException {
        final int maxDoc = 65536 * 2 + between(1, 1000);
        final BytesRef[][] docSlots = sparseSlots(maxDoc, "red", "green", "blue");
        assertCounts(docSlots);
    }

    /** When the policy is enabled, the census vocabulary matches a standalone survey. */
    public void testVocabularyMatchesSurvey() throws IOException {
        final BytesRef[][] docSlots = denseSlots(200, "cat", "dog", "bird");
        assertVocabulary(docSlots);
    }

    /** The vocabulary from a sparse column matches a standalone survey. */
    public void testSparseVocabularyMatchesSurvey() throws IOException {
        final BytesRef[][] docSlots = sparseSlots(200, "one", "two", "three");
        assertVocabulary(docSlots);
    }

    /** When the census only counts (doSurvey=false), terms() is null. */
    public void testCountOnlyReturnsNullTerms() throws IOException {
        final BytesRef[][] docSlots = denseSlots(100, "x", "y");
        final StringCensus census = new StringCensus(false, ROOMY_POLICY);
        census.walk(cursor(docSlots));
        assertNull(census.terms());
    }

    /** needed() is false only when both totals and vocabulary are already provided. */
    public void testNeeded() throws IOException {
        final StringColumnValues.Totals someTotals = new StringColumnValues.Totals(1, 1, 0);
        final Vocabulary.Terms someTerms = Vocabulary.survey(cursor(new BytesRef[][] { { new BytesRef("a") } }), ROOMY_POLICY);
        assumeTrue("policy kept the vocabulary", someTerms != null);

        // Totals missing → always needed.
        assertTrue(StringCensus.needed(null, someTerms, ROOMY_POLICY));
        assertTrue(StringCensus.needed(null, null, ROOMY_POLICY));
        assertTrue(StringCensus.needed(null, someTerms, DictionaryPolicy.NONE));

        // Totals present, policy disabled → not needed.
        assertFalse(StringCensus.needed(someTotals, null, DictionaryPolicy.NONE));
        assertFalse(StringCensus.needed(someTotals, someTerms, DictionaryPolicy.NONE));

        // Totals present, policy enabled, vocabulary missing → needed for survey.
        assertTrue(StringCensus.needed(someTotals, null, ROOMY_POLICY));

        // Totals present, policy enabled, vocabulary present → not needed.
        assertFalse(StringCensus.needed(someTotals, someTerms, ROOMY_POLICY));
    }

    // --- helpers ---

    private void assertCounts(BytesRef[][] docSlots) throws IOException {
        final int expectedDocs = numDocsWithField(docSlots);
        final long expectedValues = numValues(docSlots);
        final long expectedNulls = numNullSlots(docSlots);

        // Counting-only census.
        final StringCensus countOnly = new StringCensus(false, ROOMY_POLICY);
        countOnly.walk(cursor(docSlots));
        final StringColumnValues.Totals totals = countOnly.totals();
        assertEquals("numDocsWithField", expectedDocs, totals.numDocsWithField());
        assertEquals("numValues", expectedValues, totals.numValues());
        assertEquals("numNullSlots", expectedNulls, totals.numNullSlots());

        // Surveying census counts match too.
        final StringCensus surveying = new StringCensus(true, ROOMY_POLICY);
        surveying.walk(cursor(docSlots));
        final StringColumnValues.Totals surveyTotals = surveying.totals();
        assertEquals("numDocsWithField (survey)", expectedDocs, surveyTotals.numDocsWithField());
        assertEquals("numValues (survey)", expectedValues, surveyTotals.numValues());
        assertEquals("numNullSlots (survey)", expectedNulls, surveyTotals.numNullSlots());
    }

    private void assertVocabulary(BytesRef[][] docSlots) throws IOException {
        final Vocabulary.Terms expected = Vocabulary.survey(cursor(docSlots), ROOMY_POLICY);

        final StringCensus census = new StringCensus(true, ROOMY_POLICY);
        census.walk(cursor(docSlots));
        final Vocabulary.Terms actual = census.terms();

        if (expected == null) {
            assertNull("census should agree survey found nothing", actual);
            return;
        }
        assertNotNull("census should have found terms", actual);
        assertEquals("vocabulary size", expected.size(), actual.size());
        final org.apache.lucene.util.BytesRef scratch1 = new BytesRef();
        final org.apache.lucene.util.BytesRef scratch2 = new BytesRef();
        for (int ordinal = 0; ordinal < expected.size(); ordinal++) {
            expected.terms().get(expected.sortedIds()[ordinal], scratch1);
            actual.terms().get(actual.sortedIds()[ordinal], scratch2);
            assertEquals("term at ordinal " + ordinal, scratch1, scratch2);
        }
    }

    private static BytesRef[][] denseSlots(int maxDoc, String... pool) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            docSlots[d] = new BytesRef[] { new BytesRef(pool[d % pool.length]) };
        }
        return docSlots;
    }

    private static BytesRef[][] sparseSlots(int maxDoc, String... pool) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            if (d % 2 == 0) {
                docSlots[d] = new BytesRef[] { new BytesRef(pool[d % pool.length]) };
            }
        }
        return docSlots;
    }
}
