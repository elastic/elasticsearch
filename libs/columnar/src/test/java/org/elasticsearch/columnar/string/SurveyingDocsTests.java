/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests that {@link SurveyingDocs} produces the same vocabulary as a standalone {@link Vocabulary#survey}
 * over the same values, under a variety of column shapes — including sparse, multi-valued, and null slots.
 */
public class SurveyingDocsTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.0, 0.0);

    /**
     * A single-valued sparse column: the terms the combined pass finds must equal the terms a standalone survey finds.
     */
    public void testCombinedSurveyMatchesStandaloneSingleValued() throws IOException {
        final BytesRef[][] docSlots = sparseSingleValued();
        assertCombinedMatchesStandalone(docSlots);
    }

    /**
     * A multi-valued sparse column: multi-value columns expose the per-document value loop in
     * {@link SurveyingDocs#nextDoc()} to more than one {@code accept} call per document.
     */
    public void testCombinedSurveyMatchesStandaloneMultiValued() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(200, 1000), 6, true, false);
        addFrequentTerms(docSlots);
        assertCombinedMatchesStandalone(docSlots);
    }

    /** Null slots must be silently ignored by the surveyor — not credited to any term. */
    public void testCombinedSurveyMatchesStandaloneWithNulls() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(200, 1000), 6, true, true);
        addFrequentTerms(docSlots);
        assertCombinedMatchesStandalone(docSlots);
    }

    /**
     * A column of entirely distinct values: the combined survey must agree with the standalone survey that
     * there is no vocabulary worth keeping.
     */
    public void testNoVocabularyWhenAllValuesDistinct() throws IOException {
        final int maxDoc = between(200, 1000);
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (randomBoolean()) {
                docSlots[doc] = new BytesRef[] { new BytesRef("id-" + doc) };
            }
        }
        assertCombinedMatchesStandalone(docSlots);
    }

    // ---- helpers ----

    private void assertCombinedMatchesStandalone(BytesRef[][] docSlots) throws IOException {
        final int numDocsWithField = numDocsWithField(docSlots);
        // The combined path only runs for a sparse column (numDocsWithField < maxDoc),
        // so add a phantom document to ensure that even if all docs have a field.
        final int maxDoc = numDocsWithField < docSlots.length ? docSlots.length : docSlots.length + 1;

        // Standalone survey.
        final Vocabulary.Terms standalone = Vocabulary.survey(cursor(docSlots), ROOMY);

        // Combined path: SurveyingDocs wrapped around the cursor, fed through ColumnIteratorWriter.
        final Vocabulary.Surveyor surveyor = Vocabulary.surveyor(ROOMY);
        final SurveyingDocs docs = new SurveyingDocs(cursor(docSlots), surveyor, numDocsWithField);
        final ByteBuffersDataOutput buf = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(buf, "test", "test")) {
            final ColumnIteratorMetadata meta = ColumnIteratorWriter.write(docs, numDocsWithField, maxDoc, out);
            assertNotNull("sparse column must write a presence structure", meta);
        }
        final Vocabulary.Terms combined = docs.finish();

        // Both or neither should find a vocabulary.
        assertEquals("both paths should agree on whether a vocabulary exists", standalone == null, combined == null);
        if (standalone == null) {
            return;
        }

        // The terms must be identical in count and content.
        assertEquals("vocabulary size", standalone.size(), combined.size());
        final BytesRef sa = new BytesRef();
        final BytesRef cb = new BytesRef();
        for (int ordinal = 0; ordinal < standalone.size(); ordinal++) {
            standalone.terms().get(standalone.sortedIds()[ordinal], sa);
            combined.terms().get(combined.sortedIds()[ordinal], cb);
            assertEquals("term at ordinal " + ordinal, sa, cb);
        }
    }

    /**
     * A sparse single-valued column with a handful of terms repeated many times — the shape that produces a
     * vocabulary worth keeping.
     */
    private BytesRef[][] sparseSingleValued() {
        final String[] common = { "INFO", "DEBUG", "WARN", "ERROR" };
        final int maxDoc = between(500, 2000);
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (randomBoolean()) {
                docSlots[doc] = new BytesRef[] { new BytesRef(randomFrom(common)) };
            }
        }
        return docSlots;
    }

    /**
     * Overwrites a random non-null slot in each non-null document with one of a few frequent terms, so the
     * column always has a vocabulary worth finding even after {@code randomDocSlots} has scattered random
     * distinct values through it.
     */
    private void addFrequentTerms(BytesRef[][] docSlots) {
        final String[] common = { "alpha", "bravo", "charlie" };
        for (BytesRef[] slots : docSlots) {
            if (slots == null) {
                continue;
            }
            // Find a non-null slot to overwrite.
            final List<Integer> nonNull = new ArrayList<>();
            for (int i = 0; i < slots.length; i++) {
                if (slots[i] != null) {
                    nonNull.add(i);
                }
            }
            if (nonNull.isEmpty() == false) {
                slots[randomFrom(nonNull)] = new BytesRef(randomFrom(common));
            }
        }
    }
}
