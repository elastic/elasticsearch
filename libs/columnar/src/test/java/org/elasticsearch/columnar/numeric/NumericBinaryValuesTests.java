/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarTestUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.columnar.numeric.NumericBinaryValues.INITIAL_DECODE_CAPACITY;
import static org.hamcrest.Matchers.instanceOf;

/**
 * {@link NumericBinaryValues} reads the same documents the same way whether the binary doc values are a ColumNAR column or
 * plain payloads. Each document is written as its values in order, or as {@code null} where it has none.
 */
public class NumericBinaryValuesTests extends ESTestCase {

    private static final String FIELD = "field";

    private static final long[][] DOCS = { { 7 }, null, { 3, 9, 5 }, { 4, 4 }, { Long.MAX_VALUE, Long.MIN_VALUE }, null, { -1 } };

    /** Both sources visit the same documents and give the same count and values, in the order they were written. */
    public void testPayloadsAndColumnAgree() throws IOException {
        assertReadsWritten(NumericBinaryValues.of(payloads(DOCS, new int[1])), DOCS);
        withColumn(DOCS, column -> assertReadsWritten(column, DOCS));
    }

    /**
     * The payloads are decoded once per document, however many of its values are read, and the values read are always those
     * of the current document, including after moving past one without a value.
     */
    public void testPayloadsFollowTheCurrentDocument() throws IOException {
        final int[] decodes = new int[1];
        final NumericBinaryValues values = NumericBinaryValues.of(payloads(DOCS, decodes));

        assertTrue(values.advanceExact(2));
        assertValues(values, 3, 9, 5);
        assertValues(values, 3, 9, 5);
        assertEquals(1, decodes[0]);

        assertTrue(values.advanceExact(3));
        assertValues(values, 4, 4);
        assertEquals(2, decodes[0]);

        assertFalse(values.advanceExact(5));
        assertTrue(values.advanceExact(6));
        assertValues(values, -1);
        assertEquals(3, decodes[0]);
    }

    /** A document holding more values than the decode buffer starts with, between documents holding fewer. */
    public void testMoreValuesThanInitialDecodeCapacity() throws IOException {
        final long[][] docs = { sequence(2), sequence(INITIAL_DECODE_CAPACITY + 1), sequence(1), sequence(INITIAL_DECODE_CAPACITY * 3) };
        assertReadsWritten(NumericBinaryValues.of(payloads(docs, new int[1])), docs);
        withColumn(docs, column -> assertReadsWritten(column, docs));
    }

    /** Payloads say nothing of the segment as a whole, so they never claim to be single-valued, even when every document is. */
    public void testPayloadsAreNeverSingleValued() {
        assertFalse(NumericBinaryValues.of(payloads(new long[][] { { 1 }, { 2 } }, new int[1])).isSingleValued());
    }

    /** A column is single-valued exactly when every document in it holds one value. */
    public void testColumnSingleValuedFollowsTheColumn() throws IOException {
        withColumn(new long[][] { { 1 }, null, { 2 } }, column -> assertTrue(column.isSingleValued()));
        withColumn(new long[][] { { 1 }, { 3, 2 }, { 2 } }, column -> assertFalse(column.isSingleValued()));
    }

    private static void assertReadsWritten(NumericBinaryValues values, long[][] docs) throws IOException {
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] == null) {
                continue;
            }
            assertEquals(doc, values.nextDoc());
            assertValues(values, docs[doc]);
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.nextDoc());
    }

    private static void assertValues(NumericBinaryValues values, long... expected) throws IOException {
        final long[] actual = new long[values.docValueCount()];
        for (int i = 0; i < actual.length; i++) {
            actual[i] = values.valueAt(i);
        }
        assertArrayEquals("doc " + values.docID(), expected, actual);
    }

    private static long[] sequence(int count) {
        final long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = count - i;
        }
        return values;
    }

    /** Writes {@code docs} as a ColumNAR column and hands its values to {@code check}. */
    private void withColumn(long[][] docs, CheckedConsumer<NumericBinaryValues, IOException> check) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig(null).setCodec(ColumnarTestUtils.columnarCodec(ColumnarFieldType.LONG));
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (long[] values : docs) {
                final Document doc = new Document();
                if (values != null) {
                    doc.add(new BinaryDocValuesField(FIELD, encode(values)));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final LeafReader leaf = getOnlyLeafReader(reader);
                final BinaryDocValues binary = leaf.getBinaryDocValues(FIELD);
                assertThat(binary, instanceOf(ColumnarNumericBinaryDocValues.class));
                check.accept(NumericBinaryValues.of(binary));
            }
        }
    }

    /** Plain payloads over {@code docs}, not a column, counting in {@code decodes} each time a payload is read. */
    private static BinaryDocValues payloads(long[][] docs, int[] decodes) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                decodes[0]++;
                return encode(docs[doc]);
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return docs[target] != null;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                doc = target;
                while (doc < docs.length && docs[doc] == null) {
                    doc++;
                }
                if (doc >= docs.length) {
                    doc = NO_MORE_DOCS;
                }
                return doc;
            }

            @Override
            public long cost() {
                return docs.length;
            }
        };
    }

    private static BytesRef encode(long[] values) {
        return BytesRef.deepCopyOf(NumericBinaryPayload.encode(values, values.length, new BytesRefBuilder()));
    }
}
