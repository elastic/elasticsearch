/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumnarTestUtils.CountingSupplier;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;

import java.io.IOException;

/**
 * Asserts the number of cursor opens (passes over the source) {@link StringColumnWriter#write} makes for each
 * column shape. Every saving this change promises is invisible to a round-trip test, so these pass-count
 * assertions are the regression guard: each commit that eliminates a pass drops one of these numbers by one.
 *
 * <p>Counts are measured at the {@link StringColumnWriter#write} boundary only — the one extra cursor the
 * consumer's counting pass builds (when totals are not already recorded) is a separate concern and is covered
 * by {@link org.elasticsearch.columnar.StringColumnMergeTests}.
 *
 * <p>Target counts (each number should fall to this after all commits):
 * <ul>
 *   <li>Dense, plain: 1 (already optimal)</li>
 *   <li>Sparse, plain: 2 → 1</li>
 *   <li>Dense, dictionary, vocabulary known: 2 → 1</li>
 *   <li>Sparse, dictionary, vocabulary known: 3 → 1</li>
 *   <li>Dense or sparse, dictionary, survey needed: 3 → 2</li>
 * </ul>
 */
public class StringColumnPassCountTests extends ColumnarStringTestCase {

    /** A dictionary policy that is generous enough to always keep the dictionary for our test values. */
    private static final DictionaryPolicy ROOMY_POLICY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    private static final StringColumnOptions PLAIN_OPTIONS = new StringColumnOptions(
        DictionaryPolicy.NONE,
        ChunkCodec.IDENTITY,
        defaultSizes()
    );
    private static final StringColumnOptions DICTIONARY_OPTIONS = new StringColumnOptions(
        ROOMY_POLICY,
        ChunkCodec.IDENTITY,
        defaultSizes()
    );

    private static StringColumnOptions.Sizes defaultSizes() {
        return new StringColumnOptions.Sizes(
            128,
            ChunkBounds.ofBytes(64 * 1024),
            ChunkBounds.ofBytes(64 * 1024),
            StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
            StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
            StringColumnOptions.DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
            StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE
        );
    }

    /** Dense plain: presence short-circuits; only the value pass runs. */
    public void testDensePlainPassCount() throws IOException {
        final BytesRef[][] docSlots = denseSlots(100, "apple", "banana", "cherry");
        try (Directory dir = newDirectory()) {
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, PLAIN_OPTIONS, null);
            assertEquals("dense plain: 1 pass (values only)", 1, counter.count());
        }
    }

    /** Sparse plain: one presence pass plus one value pass. */
    public void testSparsePlainPassCount() throws IOException {
        final BytesRef[][] docSlots = sparseSlots(200, "alpha", "beta", "gamma");
        try (Directory dir = newDirectory()) {
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, PLAIN_OPTIONS, null);
            assertEquals("sparse plain: 2 passes (presence + values)", 2, counter.count());
        }
    }

    /**
     * Dense dictionary, vocabulary known: presence short-circuits; one value pass plus the ordinals replay.
     * This is the shape of a merge when all inputs are ours and vocabulary is derived without a walk.
     */
    public void testDenseDictionaryKnownVocabPassCount() throws IOException {
        final BytesRef[][] docSlots = denseSlots(100, "red", "green", "blue");
        try (Directory dir = newDirectory()) {
            final Vocabulary.Terms known = surveyUncounted(docSlots);
            assumeTrue("policy kept the vocabulary", known != null);
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, DICTIONARY_OPTIONS, known);
            assertEquals("dense dictionary, known vocab: 2 passes (values + ordinals)", 2, counter.count());
        }
    }

    /**
     * Sparse dictionary, vocabulary known: one presence pass, one value pass, one ordinals replay.
     * This is the shape of a merge when all inputs are ours and vocabulary is derived without a walk.
     */
    public void testSparseDictionaryKnownVocabPassCount() throws IOException {
        final BytesRef[][] docSlots = sparseSlots(200, "red", "green", "blue");
        try (Directory dir = newDirectory()) {
            final Vocabulary.Terms known = surveyUncounted(docSlots);
            assumeTrue("policy kept the vocabulary", known != null);
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, DICTIONARY_OPTIONS, known);
            assertEquals("sparse dictionary, known vocab: 3 passes (presence + values + ordinals)", 3, counter.count());
        }
    }

    /**
     * Dense dictionary, survey needed: survey, value pass, ordinals replay; presence short-circuits.
     * Uses {@code assumeTrue} to skip when the policy decides to stay plain (not the shape under test).
     */
    public void testDenseDictionarySurveyPassCount() throws IOException {
        final BytesRef[][] docSlots = denseSlots(100, "cat", "dog", "bird");
        try (Directory dir = newDirectory()) {
            final Vocabulary.Terms known = surveyUncounted(docSlots);
            assumeTrue("policy kept the vocabulary", known != null);
            // Pass null to force the survey to happen inside the writer.
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, DICTIONARY_OPTIONS, null);
            assertEquals("dense dictionary survey: 3 passes (survey + values + ordinals)", 3, counter.count());
        }
    }

    /**
     * Sparse dictionary, survey needed: combined presence+survey pass, value pass, ordinals replay.
     * Uses {@code assumeTrue} to skip when the policy decides to stay plain.
     */
    public void testSparseDictionarySurveyPassCount() throws IOException {
        final BytesRef[][] docSlots = sparseSlots(200, "cat", "dog", "bird");
        try (Directory dir = newDirectory()) {
            final Vocabulary.Terms known = surveyUncounted(docSlots);
            assumeTrue("policy kept the vocabulary", known != null);
            final var counter = new CountingSupplier<>(() -> cursor(docSlots));
            writeWith(dir, docSlots, counter, DICTIONARY_OPTIONS, null);
            assertEquals("sparse dictionary survey: 3 passes (combined presence+survey + values + ordinals)", 3, counter.count());
        }
    }

    // --- helpers ---

    private void writeWith(
        Directory dir,
        BytesRef[][] docSlots,
        CountingSupplier<StringColumnValues> counter,
        StringColumnOptions options,
        Vocabulary.Terms known
    ) throws IOException {
        try (IndexOutput out = dir.createTempOutput("test", "cnd", IOContext.DEFAULT)) {
            StringColumnWriter.write(
                docSlots.length,
                numDocsWithField(docSlots),
                numValues(docSlots),
                numNullSlots(docSlots),
                counter,
                options,
                known,
                dir,
                IOContext.DEFAULT,
                out
            );
        }
    }

    /**
     * Surveys {@code docSlots} without counting the cursor open — called before the counting supplier is
     * created, so it does not pollute the count. Returns null when the policy would reject the vocabulary
     * (meaning the column would go plain, which is not the shape under test).
     */
    private static Vocabulary.Terms surveyUncounted(BytesRef[][] docSlots) throws IOException {
        final Vocabulary.Terms terms = Vocabulary.survey(cursor(docSlots), ROOMY_POLICY);
        if (terms == null) {
            return null;
        }
        return ROOMY_POLICY.worthKeeping(terms.coverage(), terms.dictionaryBytes(), terms.columnBytes()) ? terms : null;
    }

    /** All {@code maxDoc} documents have one value, cycling through {@code termPool}. */
    private static BytesRef[][] denseSlots(int maxDoc, String... termPool) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            docSlots[d] = new BytesRef[] { new BytesRef(termPool[d % termPool.length]) };
        }
        return docSlots;
    }

    /** Roughly half the documents have one value, cycling through {@code termPool}. */
    private static BytesRef[][] sparseSlots(int maxDoc, String... termPool) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            if (d % 2 == 0) {
                docSlots[d] = new BytesRef[] { new BytesRef(termPool[d % termPool.length]) };
            }
        }
        return docSlots;
    }
}
