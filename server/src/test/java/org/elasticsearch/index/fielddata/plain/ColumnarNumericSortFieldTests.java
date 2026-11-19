/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.index.mapper.ColumnarNumericDocValuesField;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ColumnarNumericSortFieldTests extends ESTestCase {

    private static final String FIELD = "field";

    /** A document's values, and the keys it sorts by in {@code min} and {@code max} mode, or {@code null} keys if it has none. */
    private record Doc(List<Long> values, Long min, Long max) {}

    private static final List<Doc> DOCS = List.of(
        new Doc(List.of(7L), 7L, 7L),
        new Doc(List.of(3L, 9L, 5L), 3L, 9L),
        new Doc(List.of(), null, null),
        new Doc(List.of(Long.MAX_VALUE, Long.MIN_VALUE), Long.MIN_VALUE, Long.MAX_VALUE),
        new Doc(List.of(4L, 4L), 4L, 4L),
        new Doc(List.of(-1L), -1L, -1L),
        // More values than NumericBinaryValues.INITIAL_DECODE_CAPACITY.
        new Doc(List.of(12L, 11L, 10L, 20L, 13L, 14L, 15L, 16L, 17L, 18L, -2L), -2L, 20L)
    );

    /** Payloads that haven't been through the codec, such as the buffered ones a segment is sorted by at flush. */
    public void testSortKeysFromPayloads() throws IOException {
        assertSortKeys(TestUtil.getDefaultCodec(), false);
    }

    /** The same documents read out of a real column give the same keys. */
    public void testSortKeysFromColumn() throws IOException {
        assertSortKeys(columnarCodec(), true);
    }

    private void assertSortKeys(Codec codec, boolean column) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(codec))) {
            for (Doc doc : DOCS) {
                w.addDocument(document(doc.values));
            }
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                final LeafReader leaf = getOnlyLeafReader(reader);
                assertThat(
                    leaf.getBinaryDocValues(FIELD),
                    column ? instanceOf(ColumnarNumericBinaryDocValues.class) : not(instanceOf(ColumnarNumericBinaryDocValues.class))
                );
                for (boolean maxMode : new boolean[] { false, true }) {
                    final NumericDocValues keys = sortField(maxMode).sortKeys(leaf);
                    for (int d = 0; d < DOCS.size(); d++) {
                        final Long expected = maxMode ? DOCS.get(d).max : DOCS.get(d).min;
                        if (expected == null) {
                            assertFalse("doc " + d + " maxMode=" + maxMode + " has no key", keys.advanceExact(d));
                        } else {
                            assertTrue("doc " + d + " maxMode=" + maxMode + " has a key", keys.advanceExact(d));
                            assertEquals("doc " + d + " maxMode=" + maxMode, expected.longValue(), keys.longValue());
                        }
                    }
                }
            }
        }
    }

    public void testIsMaybeMultiValuedWithoutTheField() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(columnarCodec()))) {
            w.addDocument(new LuceneDocument());
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertFalse("no document holds the field", sortField(false).isMaybeMultiValued(getOnlyLeafReader(reader)));
            }
        }
    }

    public void testIsMaybeMultiValuedWithSingleValues() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(columnarCodec()))) {
            w.addDocument(document(List.of(1L)));
            w.addDocument(document(List.of()));
            w.addDocument(document(List.of(2L)));
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertFalse(sortField(false).isMaybeMultiValued(getOnlyLeafReader(reader)));
            }
        }
    }

    public void testIsMaybeMultiValuedWithOneMultiValuedDocument() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(columnarCodec()))) {
            w.addDocument(document(List.of(1L)));
            w.addDocument(document(List.of(3L, 2L)));
            w.addDocument(document(List.of(4L)));
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertTrue(sortField(false).isMaybeMultiValued(getOnlyLeafReader(reader)));
            }
        }
    }

    /** Payloads that are not a column, such as those of the default codec, say nothing of the segment, so it may be multi-valued. */
    public void testIsMaybeMultiValuedWithoutAColumn() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            w.addDocument(document(List.of(1L)));
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertTrue(sortField(false).isMaybeMultiValued(getOnlyLeafReader(reader)));
            }
        }
    }

    /** Lucene compares sorts with {@code equals}, so each property has to tell two sorts apart. */
    public void testEquals() {
        final ColumnarNumericSortField sortField = new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, false, Long.MAX_VALUE);
        final ColumnarNumericSortField same = new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, false, Long.MAX_VALUE);
        assertEquals(sortField, same);
        assertEquals(sortField.hashCode(), same.hashCode());
        assertNotEquals(sortField, new ColumnarNumericSortField("other", SortField.Type.LONG, false, false, Long.MAX_VALUE));
        assertNotEquals(sortField, new ColumnarNumericSortField(FIELD, SortField.Type.LONG, true, false, Long.MAX_VALUE));
        assertNotEquals(sortField, new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, true, Long.MAX_VALUE));
        assertNotEquals(sortField, new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, false, Long.MIN_VALUE));
        assertNotEquals(sortField, new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, false, null));
    }

    /** A plain sort on the same field, type and missing value isn't equal to one that reads the column. */
    public void testNotEqualToPlainSortField() {
        Long missing = randomBoolean() ? null : randomLong();
        boolean reverse = randomBoolean();
        ColumnarNumericSortField columnar = new ColumnarNumericSortField(FIELD, SortField.Type.LONG, reverse, randomBoolean(), missing);
        SortField plain = new SortField(FIELD, SortField.Type.LONG, reverse, missing);
        assertNotEquals(columnar, plain);
    }

    /** A segment sorted by the field records it in its segment info, which is read back through the SPI-registered provider. */
    public void testProviderRoundTrip() throws IOException {
        Long missing = randomBoolean() ? null : randomFrom(Long.MIN_VALUE, Long.MAX_VALUE, randomLong());
        ColumnarNumericSortField original = new ColumnarNumericSortField(
            randomAlphaOfLength(5),
            SortField.Type.LONG,
            randomBoolean(),
            randomBoolean(),
            missing
        );
        SortFieldProvider provider = SortFieldProvider.forName(ColumnarNumericSortField.PROVIDER_NAME);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        provider.writeSortField(original, out);
        SortField restored = provider.readSortField(out.toDataInput());
        assertEquals(original, restored);
        assertEquals(SortField.Type.LONG, restored.getType());
        assertEquals(original.maxMode(), ((ColumnarNumericSortField) restored).maxMode());
    }

    /**
     * The layout segment info holds, pinned byte by byte: segments written today have to read back after the provider changes.
     * The field and the type's name are each a length and UTF-8 bytes; the order, the mode and whether a missing value follows
     * are each a little-endian int; the missing value is a little-endian long.
     */
    public void testProviderLayoutWithMissingValue() throws IOException {
        assertLayout(
            new ColumnarNumericSortField("f", SortField.Type.LONG, true, false, Long.MIN_VALUE),
            new byte[] { 1, 'f', 4, 'L', 'O', 'N', 'G', 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0x80 }
        );
    }

    /** As {@link #testProviderLayoutWithMissingValue}, without a missing value. */
    public void testProviderLayoutWithoutMissingValue() throws IOException {
        assertLayout(
            new ColumnarNumericSortField("f", SortField.Type.LONG, false, true, null),
            new byte[] { 1, 'f', 4, 'L', 'O', 'N', 'G', 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0 }
        );
    }

    private static void assertLayout(ColumnarNumericSortField sortField, byte[] bytes) throws IOException {
        SortFieldProvider provider = SortFieldProvider.forName(ColumnarNumericSortField.PROVIDER_NAME);
        ColumnarNumericSortField read = (ColumnarNumericSortField) provider.readSortField(new ByteArrayDataInput(bytes));
        assertEquals(sortField, read);
        assertEquals(sortField.maxMode(), read.maxMode());
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        provider.writeSortField(sortField, out);
        assertArrayEquals(bytes, out.toArrayCopy());
    }

    /** Only long fields are stored with ColumNAR so far. */
    public void testOnlyLongIsSupported() {
        SortField.Type type = randomValueOtherThan(SortField.Type.LONG, () -> randomFrom(SortField.Type.values()));
        expectThrows(IllegalArgumentException.class, () -> new ColumnarNumericSortField(FIELD, type, false, false, null));
    }

    private static ColumnarNumericSortField sortField(boolean maxMode) {
        return new ColumnarNumericSortField(FIELD, SortField.Type.LONG, false, maxMode, null);
    }

    private static LuceneDocument document(List<Long> values) {
        final LuceneDocument doc = new LuceneDocument();
        for (long value : values) {
            ColumnarNumericDocValuesField.recordValue(doc, FIELD, value);
        }
        return doc;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        // The type is injected rather than read from a field attribute: these documents are built through
        // ColumnarNumericDocValuesField, which is the mapper's field and carries no codec attribute.
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(
            (fieldName, fieldType) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.LONG,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE
        );
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
