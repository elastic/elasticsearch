/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;

/**
 * What a column says about its own shape, which is what lets a single value check rewrite itself away
 * rather than ask every document.
 */
public class ColumnarPayloadSortedBinaryDocValuesTests extends ESTestCase {

    private static final String FIELD = "kw";

    public void testOneSlotADocumentIsSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { "b" }, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
            assertEquals(SortedBinaryDocValues.Sparsity.DENSE, values.getSparsity());
        });
    }

    /** A null slot is no value, and single valued says at most one, so a column holding them still qualifies. */
    public void testANullSlotIsStillSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { null }, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
        });
    }

    public void testASecondSlotIsNotSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { "b", "c" } }, values -> {
            assertNotEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
        });
    }

    /** A document the field is absent from leaves the column short of the segment, which is sparse. */
    public void testAColumnMissingDocumentsIsSparse() throws IOException {
        withColumn(new String[][] { { "a" }, null, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.Sparsity.SPARSE, values.getSparsity());
        });
    }

    /** What an aggregation that only counts does: advance, take the count, never read a value. */
    public void testCountingReadsNoValue() throws IOException {
        final String[][] docs = { { "a" }, { "b", "c" }, { null }, { "d", null, "e" }, null, { "f", "g", "h" } };
        withColumn(docs, values -> {
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertTrue(values.advanceExact(1));
            assertEquals(2, values.docValueCount());
            assertFalse("a document of one null slot holds no value", values.advanceExact(2));
            assertEquals(0, values.docValueCount());
            assertTrue(values.advanceExact(3));
            assertEquals("the null slot does not count", 2, values.docValueCount());
            assertFalse("the field is absent", values.advanceExact(4));
            assertTrue(values.advanceExact(5));
            assertEquals(3, values.docValueCount());
        });
    }

    /** The values are still there once something asks for them, sorted, after a count that did not. */
    public void testValuesSurviveACountThatDidNotReadThem() throws IOException {
        withColumn(new String[][] { { "b", "a" }, { "d", null, "c" } }, values -> {
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertTrue(values.advanceExact(1));
            assertEquals(2, values.docValueCount());
            assertEquals(new BytesRef("c"), values.nextValue());
            assertEquals(new BytesRef("d"), values.nextValue());
            assertTrue(values.advanceExact(0));
            assertEquals(new BytesRef("a"), values.nextValue());
            assertEquals(new BytesRef("b"), values.nextValue());
        });
    }

    /** A document holding one value is handed it straight off the column, and every document gets its own. */
    public void testEveryDocumentReadsItsOwnValue() throws IOException {
        final String[][] docs = { { "a" }, { "b" }, { null }, { "c" }, null, { "d" } };
        withColumn(docs, values -> {
            assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
            assertTrue(values.advanceExact(0));
            assertEquals(new BytesRef("a"), values.nextValue());
            assertTrue(values.advanceExact(1));
            assertEquals(new BytesRef("b"), values.nextValue());
            assertFalse(values.advanceExact(2));
            assertTrue(values.advanceExact(3));
            assertEquals(new BytesRef("c"), values.nextValue());
            assertFalse(values.advanceExact(4));
            assertTrue(values.advanceExact(5));
            assertEquals(new BytesRef("d"), values.nextValue());
        });
    }

    /** Reading a document twice gives the same value both times. */
    public void testADocumentCanBeReadAgain() throws IOException {
        withColumn(new String[][] { { "a" }, { "b" } }, values -> {
            assertTrue(values.advanceExact(1));
            assertEquals(new BytesRef("b"), values.nextValue());
            assertTrue(values.advanceExact(1));
            assertEquals(new BytesRef("b"), values.nextValue());
        });
    }

    /** This surface sorts but does not deduplicate, so a repeated value comes back as many times as it was written. */
    public void testDuplicatesAreKept() throws IOException {
        withColumn(new String[][] { { "b", "a", "b" } }, values -> {
            assertTrue(values.advanceExact(0));
            assertEquals(3, values.docValueCount());
            assertEquals(new BytesRef("a"), values.nextValue());
            assertEquals(new BytesRef("b"), values.nextValue());
            assertEquals(new BytesRef("b"), values.nextValue());
        });
    }

    /** Counting one document and reading the next, back and forth, over a column holding both shapes. */
    public void testCountingAndReadingInterleaved() throws IOException {
        withColumn(new String[][] { { "b", "a" }, { "c" }, { "e", null, "d" }, { "f" } }, values -> {
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertTrue(values.advanceExact(1));
            assertEquals(new BytesRef("c"), values.nextValue());
            assertTrue(values.advanceExact(2));
            assertEquals(2, values.docValueCount());
            assertEquals(new BytesRef("d"), values.nextValue());
            assertEquals(new BytesRef("e"), values.nextValue());
            assertTrue(values.advanceExact(3));
            assertEquals(new BytesRef("f"), values.nextValue());
            assertTrue(values.advanceExact(0));
            assertEquals(new BytesRef("a"), values.nextValue());
            assertEquals(new BytesRef("b"), values.nextValue());
        });
    }

    private interface Check {
        void check(SortedBinaryDocValues values) throws IOException;
    }

    /** {@code null} in place of a document's slots writes no field for it at all. */
    private void withColumn(String[][] docs, Check check) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String[] slots : docs) {
                    final Document doc = new Document();
                    if (slots != null) {
                        doc.add(new Field(FIELD, encode(slots), binaryDocValuesType()));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertThat("the field is a column", leaf.getBinaryDocValues(FIELD), instanceOf(StringColumnSource.class));
                check.check(ColumnarPayloadSortedBinaryDocValues.from(leaf, FIELD));
            }
        }
    }

    private static BytesRef encode(String[] slots) {
        final BytesRef[] refs = new BytesRef[slots.length];
        for (int i = 0; i < slots.length; i++) {
            refs[i] = slots[i] == null ? null : new BytesRef(slots[i]);
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(Arrays.asList(refs)));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
