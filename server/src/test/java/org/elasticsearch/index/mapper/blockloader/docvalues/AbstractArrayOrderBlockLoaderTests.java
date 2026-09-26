/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.TestDocumentParserContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Shared tests for the offsets-aware {@code ArrayOrder} reader path inside each {@code AbstractXBlockLoader}. Each subclass binds a value
 * type {@code V} and provides three hooks: a random value generator, a doc-values writer, and a loader factory.
 */
public abstract class AbstractArrayOrderBlockLoaderTests<V extends Comparable<V>> extends ESTestCase {

    protected static final String FIELD = "field";
    protected static final String OFFSETS = FieldArrayContext.offsetsFieldName(FIELD);
    protected static final CircuitBreaker BREAKER = new NoopCircuitBreaker("test");

    private static final int ARRAY_LENGTH = 8;

    /** Generates a fresh value of type V. */
    protected abstract V randomValue();

    /** How to write one distinct value to the field's doc values on the {@link LuceneDocument}. */
    protected abstract BiConsumer<LuceneDocument, V> addField();

    /** Construct the loader under test, configured to read offsets. */
    protected abstract BlockDocValuesReader.DocValuesBlockLoader newLoader(String fieldName);

    /**
     * The block entry each family's fallback (non-ArrayOrder) read produces when the given distinct values are stored without an offsets
     * companion field.
     */
    protected abstract Object expectedFallbackShape(List<V> insertedDistinctValues);

    public final void testArrivalOrderWithDuplicates() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(2);
        List<V> valueArray = generateArray(alphabet, ARRAY_LENGTH);
        assertSingleDocBlock(offsets -> valueArray.forEach(v -> offsets.recordOffset(OFFSETS, v)), distinctSorted(valueArray), valueArray);
    }

    public final void testInlineNullDropped() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(2);
        V val1 = alphabet.get(0);
        V val2 = alphabet.get(1);

        assertSingleDocBlock(offsets -> {
            offsets.recordOffset(OFFSETS, val1);
            offsets.recordNull(OFFSETS);
            offsets.recordOffset(OFFSETS, val2);
        }, distinctSorted(alphabet), alphabet);
    }

    public final void testSingleNonNullInArray() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(1);
        V val = alphabet.get(0);

        // three offset slots but only the middle is non-null — emit must skip the multi-value position-entry path and write the lone
        // value directly (block.get(0) must return the bare value, not a singleton list)
        assertSingleDocBlock(offsets -> {
            offsets.recordNull(OFFSETS);
            offsets.recordOffset(OFFSETS, val);
            offsets.recordNull(OFFSETS);
        }, alphabet, val);
    }

    public final void testAllNullArrayBecomesNullPosition() throws IOException {
        assertSingleDocBlock(offsets -> {
            offsets.recordNull(OFFSETS);
            offsets.recordNull(OFFSETS);
        }, List.of(), null);
    }

    public final void testNoOffsetsFallback() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(2);
        assertSingleFallbackDoc(alphabet, expectedFallbackShape(alphabet));
    }

    public final void testSingleValuedSegmentSkipsArrayOrder() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(2);
        V val1 = alphabet.get(0);
        V val2 = alphabet.get(1);

        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig())) {
            // two docs, each with exactly one value plus an offsets entry
            FieldArrayContext offsets0 = new FieldArrayContext();
            offsets0.recordOffset(OFFSETS, val1);
            iw.addDocument(buildOffsetsDoc(offsets0, List.of(val1), addField()));
            FieldArrayContext offsets1 = new FieldArrayContext();
            offsets1.recordOffset(OFFSETS, val2);
            iw.addDocument(buildOffsetsDoc(offsets1, List.of(val2), addField()));
            iw.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                try (var r = newLoader(FIELD).reader(BREAKER, ctx)) {
                    // single-value-per-doc fast path: dispatcher must elide every family's ArrayOrder reader regardless of which
                    // alternate shape (Singleton, BytesRefsFromBinary, ...) it lands on instead
                    assertFalse(
                        "dispatcher must elide ArrayOrder for single-valued segments, got " + r,
                        r instanceof AbstractNumericBlockLoader.ArrayOrder<?> || r instanceof BytesRefsFromOrdsBlockLoader.ArrayOrder
                    );
                    TestBlock block = (TestBlock) r.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false);
                    assertEquals(val1, block.get(0));
                    assertEquals(val2, block.get(1));
                }
            }
        }
    }

    public final void testMissingDocProducesNull() throws IOException {
        List<V> alphabet = generateDistinctAlphabet(2);
        V val1 = alphabet.get(0);
        V val2 = alphabet.get(1);

        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig())) {
            // doc 0 has values
            FieldArrayContext offsets0 = new FieldArrayContext();
            offsets0.recordOffset(OFFSETS, val1);
            iw.addDocument(buildOffsetsDoc(offsets0, List.of(val1), addField()));
            // empty doc to force advance past it
            iw.addDocument(new Document());
            // doc 2 has values
            FieldArrayContext offsets2 = new FieldArrayContext();
            offsets2.recordOffset(OFFSETS, val2);
            iw.addDocument(buildOffsetsDoc(offsets2, List.of(val2), addField()));
            iw.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                try (var r = newLoader(FIELD).reader(BREAKER, ctx)) {
                    TestBlock block = (TestBlock) r.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                    assertEquals(val1, block.get(0));
                    assertNull(block.get(1));
                    assertEquals(val2, block.get(2));
                }
            }
        }
    }

    /**
     * Build a single-doc segment with no offsets recorded so the loader takes its fallback (non-ArrayOrder) path. Each loader family has a
     * different fallback shape (sorted, sorted+deduped, arrival order) — callers supply the expected entry.
     */
    private void assertSingleFallbackDoc(List<V> valuesToInsert, Object expectedBlockEntry) throws IOException {
        assertSingleDocBlock(offsets -> {}, valuesToInsert, expectedBlockEntry);
    }

    /**
     * Build a single-doc segment with the given offsets and distinct values, then assert the loader's block matches the expected entry.
     * Pass {@code null} for {@code expectedBlockEntry} to assert the loader emitted a null position.
     */
    private void assertSingleDocBlock(Consumer<FieldArrayContext> recordOffsets, List<V> distinctSortedValues, Object expectedBlockEntry)
        throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            FieldArrayContext offsets = new FieldArrayContext();
            recordOffsets.accept(offsets);
            iw.addDocument(buildOffsetsDoc(offsets, distinctSortedValues, addField()));
            iw.forceMerge(1);
            try (DirectoryReader reader = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                try (var r = newLoader(FIELD).reader(BREAKER, ctx)) {
                    TestBlock block = (TestBlock) r.read(TestBlock.factory(), TestBlock.docs(0), 0, false);
                    if (expectedBlockEntry == null) {
                        assertNull(block.get(0));
                    } else {
                        assertEquals(expectedBlockEntry, block.get(0));
                    }
                }
            }
        }
    }

    /**
     * Build a document that mirrors the on-disk layout an offsets-aware ordered loader expects: the field's distinct values stored as
     * normal doc values (ex. {@code SortedSetDocValuesField}), plus a sibling field named {@code <field>.offsets} holding the encoded
     * ordinals.
     *
     * @param offsets an array context that has already had its offsets recorded
     * @param distinctValues a list of distinct values that go into the field's doc values; must be in ordinal order (so
     *                       {@code distinctValues.get(0)} matches ordinal 0 in offsets)
     * @param addField a caller-supplied lambda that knows how to write one value as a doc values field
     */
    private static <V> Document buildOffsetsDoc(FieldArrayContext offsets, List<V> distinctValues, BiConsumer<LuceneDocument, V> addField)
        throws IOException {
        // Write values into this scratch doc - some encoders, namely MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc, only accept
        // a LuceneDocument
        LuceneDocument scratch = new LuceneDocument();
        for (V val : distinctValues) {
            addField.accept(scratch, val);
        }

        var parserContext = new TestDocumentParserContext();

        // Write the offsets field and then pull it back out as an IndexableField
        offsets.addToLuceneDocument(parserContext);
        IndexableField offsetsField = parserContext.doc().getField(OFFSETS);

        // Create the actual document and populate it with the values plus the encoded offsets (when any were recorded)
        Document doc = new Document();
        for (IndexableField f : scratch.getFields()) {
            doc.add(f);
        }
        if (offsetsField != null) {
            doc.add(offsetsField);
        }

        return doc;
    }

    /**
     * Draw values until {@code n} distinct ones are accumulated.
     * <p>
     * The purpose of this method is to generate an array with duplicates. Since duplicates are dropped during storage, we must verify that
     * they're correctly reconstructed during reads.
     */
    private List<V> generateDistinctAlphabet(int n) {
        Set<V> seen = new LinkedHashSet<>();
        for (int attempt = 0; attempt < 1000; attempt++) {
            seen.add(randomValue());
            if (seen.size() >= n) {
                return new ArrayList<>(seen);
            }
        }
        throw new AssertionError("Could not generate " + n + " distinct values from randomValue() in 1000 attempts");
    }

    /**
     * Build a length-{@code length} array seeded with every alphabet value (so each appears at least once) then padded with random draws
     * from the alphabet, finally shuffled. Duplicates are guaranteed whenever {@code length > alphabet.size()}.
     */
    private List<V> generateArray(List<V> alphabet, int length) {
        assert length >= alphabet.size() : "length must accommodate every alphabet value at least once";
        List<V> array = new ArrayList<>(length);
        array.addAll(alphabet);
        for (int i = alphabet.size(); i < length; i++) {
            array.add(randomFrom(alphabet));
        }
        Collections.shuffle(array, random());
        return List.copyOf(array);
    }

    private static <T extends Comparable<T>> List<T> distinctSorted(Collection<T> in) {
        return in.stream().distinct().sorted().toList();
    }
}
