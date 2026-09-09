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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL;
import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.COLUMNAR_PAYLOAD;
import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.SEPARATE_COUNT;
import static org.hamcrest.Matchers.instanceOf;

public class MultiValuedBinaryDocValuesSortFieldTests extends ESTestCase {

    // =========================================================================
    // getSortKeyDocValues — plain BinaryDocValues (no companion .counts field)
    // =========================================================================

    /** When there is no companion {@code .counts} field, the raw BinaryDocValues are returned as-is. */
    public void testNoCounts_passthroughRawBytes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            Document doc = new Document();
            doc.add(new BinaryDocValuesField("name", new BytesRef("hello")));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("hello"), dvs.binaryValue());
            }
        }
    }

    // =========================================================================
    // getSortKeyDocValues — SeparateCount format (companion .counts field present)
    // =========================================================================

    /** count=1: binary payload is the raw term bytes; returned as-is regardless of mode. */
    public void testSingleValue_returnsRawBytes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("alice"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (boolean maxMode : new boolean[] { false, true }) {
                    BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, maxMode)
                        .getSortKeyDocValues(leaf);
                    assertTrue(dvs.advanceExact(0));
                    assertEquals("maxMode=" + maxMode, new BytesRef("alice"), dvs.binaryValue());
                }
            }
        }
    }

    /**
     * count=2, MIN mode: values stored sorted as ["bob","zebra"]; first value "bob" is the sort key.
     */
    public void testTwoValues_minMode_returnsSmallest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=2, MAX mode: values stored sorted as ["bob","zebra"]; last value "zebra" is the sort key.
     */
    public void testTwoValues_maxMode_returnsLargest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, true)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=3, MIN mode: values stored sorted as ["apple","mango","orange"]; "apple" is the sort key.
     */
    public void testThreeValues_minMode_returnsSmallest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("mango"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("apple"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("orange"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("apple"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=3, MAX mode: values stored sorted as ["apple","mango","orange"]; "orange" is the sort key.
     */
    public void testThreeValues_maxMode_returnsLargest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("mango"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("apple"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("orange"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, true)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("orange"), dvs.binaryValue());
            }
        }
    }

    // =========================================================================
    // getSortKeyDocValues — single-valued-segment fast path (skips MinMaxBinaryDocValues)
    // =========================================================================

    /**
     * When every document in a (force-merged) segment is single-valued, the {@code .counts} skipper reports
     * {@code maxValue() == 1} and {@code getSortKeyDocValues} returns the raw {@link BinaryDocValues} directly,
     * bypassing the {@code MinMaxBinaryDocValues} wrapper entirely. Verified across multiple documents so the
     * fast path is actually exercised (a single-doc segment can't distinguish it from the wrapper path).
     */
    public void testAllSingleValued_afterForceMerge_usesFastPath() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            addSingleValueDoc(w, "alice");
            addSingleValueDoc(w, "bob");
            addSingleValueDoc(w, "charlie");
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                assertEquals(1, leaf.getDocValuesSkipper("name.counts").maxValue());

                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("alice"), dvs.binaryValue());
                assertTrue(dvs.advanceExact(1));
                assertEquals(new BytesRef("bob"), dvs.binaryValue());
                assertTrue(dvs.advanceExact(2));
                assertEquals(new BytesRef("charlie"), dvs.binaryValue());
            }
        }
    }

    /**
     * A single multi-valued document mixed in with otherwise single-valued documents raises the segment-wide
     * {@code .counts} skipper max to 2, so {@code getSortKeyDocValues} falls back to the {@code MinMaxBinaryDocValues}
     * wrapper for the whole segment - which must still decode every document correctly, single- and multi-valued
     * alike, in both MIN and MAX mode.
     */
    public void testOneMultiValuedDocMixedIn_afterForceMerge_wrapperDecodesAll() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            addSingleValueDoc(w, "alice");
            LuceneDocument multiValueDoc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(multiValueDoc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(multiValueDoc, "name", new BytesRef("bob"));
            w.addDocument(multiValueDoc);
            addSingleValueDoc(w, "charlie");
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                assertEquals(2, leaf.getDocValuesSkipper("name.counts").maxValue());

                BinaryDocValues minDvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(minDvs.advanceExact(0));
                assertEquals(new BytesRef("alice"), minDvs.binaryValue());
                assertTrue(minDvs.advanceExact(1));
                assertEquals(new BytesRef("bob"), minDvs.binaryValue());
                assertTrue(minDvs.advanceExact(2));
                assertEquals(new BytesRef("charlie"), minDvs.binaryValue());

                BinaryDocValues maxDvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, true)
                    .getSortKeyDocValues(leaf);
                assertTrue(maxDvs.advanceExact(0));
                assertEquals(new BytesRef("alice"), maxDvs.binaryValue());
                assertTrue(maxDvs.advanceExact(1));
                assertEquals(new BytesRef("zebra"), maxDvs.binaryValue());
                assertTrue(maxDvs.advanceExact(2));
                assertEquals(new BytesRef("charlie"), maxDvs.binaryValue());
            }
        }
    }

    private static void addSingleValueDoc(IndexWriter w, String value) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef(value));
        w.addDocument(doc);
    }

    // =========================================================================
    // getSortKeyDocValues — ArrayOrderInlineNull format (document order, inline nulls)
    // =========================================================================

    /** A single value is stored raw in ArrayOrderInlineNull too; returned as-is regardless of mode. */
    public void testArrayOrder_singleValue_returnsRawBytes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordSingleValue(doc, "name", new BytesRef("alice"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (boolean maxMode : new boolean[] { false, true }) {
                    BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField(
                        "name",
                        false,
                        SortField.STRING_LAST,
                        maxMode,
                        ARRAY_ORDER_INLINE_NULL
                    ).getSortKeyDocValues(leaf);
                    assertTrue(dvs.advanceExact(0));
                    assertEquals("maxMode=" + maxMode, new BytesRef("alice"), dvs.binaryValue());
                }
            }
        }
    }

    /**
     * Two values stored in document order ["zebra","bob"] (not sorted, unlike SeparateCount); MIN mode must scan
     * both slots and return the smallest, "bob".
     */
    public void testArrayOrder_twoValues_minMode_returnsSmallest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField(
                    "name",
                    false,
                    SortField.STRING_LAST,
                    false,
                    ARRAY_ORDER_INLINE_NULL
                ).getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), dvs.binaryValue());
            }
        }
    }

    /** Same document-order values as above; MAX mode returns the largest, "zebra". */
    public void testArrayOrder_twoValues_maxMode_returnsLargest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField(
                    "name",
                    false,
                    SortField.STRING_LAST,
                    true,
                    ARRAY_ORDER_INLINE_NULL
                ).getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), dvs.binaryValue());
            }
        }
    }

    /**
     * A null slot in between two real values ({@code [null, "bob", "zebra"]}) must be skipped for both MIN and MAX.
     */
    public void testArrayOrder_withInlineNull_skipsNullSlot() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordNull(doc, "name");
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues minDvs = new MultiValuedBinaryDocValuesSortField(
                    "name",
                    false,
                    SortField.STRING_LAST,
                    false,
                    ARRAY_ORDER_INLINE_NULL
                ).getSortKeyDocValues(leaf);
                assertTrue(minDvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), minDvs.binaryValue());

                BinaryDocValues maxDvs = new MultiValuedBinaryDocValuesSortField(
                    "name",
                    false,
                    SortField.STRING_LAST,
                    true,
                    ARRAY_ORDER_INLINE_NULL
                ).getSortKeyDocValues(leaf);
                assertTrue(maxDvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), maxDvs.binaryValue());
            }
        }
    }

    // =========================================================================
    // getSortKeyDocValues — ColumnarPayload format (slot count carried in the blob)
    // =========================================================================

    /** Even a lone value is framed under this format, so the sort key has to be decoded rather than read raw. */
    public void testColumnarPayload_singleValue_decodesTheLoneSlot() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordSingleValue(doc, "name", new BytesRef("alice"), ValueOrdering.UNSORTED);
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (boolean maxMode : new boolean[] { false, true }) {
                    BinaryDocValues dvs = columnarSortKeys(leaf, maxMode);
                    assertTrue(dvs.advanceExact(0));
                    assertEquals("maxMode=" + maxMode, new BytesRef("alice"), dvs.binaryValue());
                }
            }
        }
    }

    /** Slots stay in document order, so both modes have to scan all of them. */
    public void testColumnarPayload_twoValues_returnsTheExtreme() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordValue(doc, "name", new BytesRef("zebra"), ValueOrdering.UNSORTED);
            ColumnarBinaryDocValuesField.recordValue(doc, "name", new BytesRef("bob"), ValueOrdering.UNSORTED);
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues minDvs = columnarSortKeys(leaf, false);
                assertTrue(minDvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), minDvs.binaryValue());

                BinaryDocValues maxDvs = columnarSortKeys(leaf, true);
                assertTrue(maxDvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), maxDvs.binaryValue());
            }
        }
    }

    /** A null slot between two real values ({@code [null, "zebra", "bob"]}) is skipped for both modes. */
    public void testColumnarPayload_withInlineNull_skipsNullSlot() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordNull(doc, "name");
            ColumnarBinaryDocValuesField.recordValue(doc, "name", new BytesRef("zebra"), ValueOrdering.UNSORTED);
            ColumnarBinaryDocValuesField.recordValue(doc, "name", new BytesRef("bob"), ValueOrdering.UNSORTED);
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues minDvs = columnarSortKeys(leaf, false);
                assertTrue(minDvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), minDvs.binaryValue());

                BinaryDocValues maxDvs = columnarSortKeys(leaf, true);
                assertTrue(maxDvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), maxDvs.binaryValue());
            }
        }
    }

    /**
     * An all-null array and an empty one both write a payload, unlike the other formats, which write no blob for them at all. Neither has
     * a value to sort on, so both must read as missing rather than sorting on the payload's framing bytes.
     */
    public void testColumnarPayload_noNonNullSlot_readsAsMissing() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument allNull = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordNull(allNull, "name");
            ColumnarBinaryDocValuesField.recordNull(allNull, "name");
            w.addDocument(allNull);

            LuceneDocument emptyArray = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordEmptyArray(emptyArray, "name");
            w.addDocument(emptyArray);

            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (boolean maxMode : new boolean[] { false, true }) {
                    BinaryDocValues dvs = columnarSortKeys(leaf, maxMode);
                    assertFalse("all-null, maxMode=" + maxMode, dvs.advanceExact(0));
                    assertNull("no key to read after a document with no value", dvs.binaryValue());
                    assertFalse("empty array, maxMode=" + maxMode, columnarSortKeys(leaf, maxMode).advanceExact(1));
                }
            }
        }
    }

    /**
     * Both index-sort drivers read sort keys with {@code nextDoc()} and take a document the cursor stepped over as having no value, so
     * the valueless documents have to be skipped by the iterator and not merely reported empty at {@code binaryValue()}.
     */
    public void testColumnarPayload_iterationSkipsValuelessDocs() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument first = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordSingleValue(first, "name", new BytesRef("alpha"), ValueOrdering.UNSORTED);
            w.addDocument(first);

            LuceneDocument allNull = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordNull(allNull, "name");
            w.addDocument(allNull);

            w.addDocument(new LuceneDocument()); // field absent entirely

            LuceneDocument last = new LuceneDocument();
            ColumnarBinaryDocValuesField.recordSingleValue(last, "name", new BytesRef("omega"), ValueOrdering.UNSORTED);
            w.addDocument(last);

            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = columnarSortKeys(leaf, false);
                assertEquals(0, dvs.nextDoc());
                assertEquals(new BytesRef("alpha"), dvs.binaryValue());
                assertEquals("doc 1 holds only a null slot and doc 2 no field", 3, dvs.nextDoc());
                assertEquals(new BytesRef("omega"), dvs.binaryValue());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dvs.nextDoc());
                assertNull("no key to read once exhausted", dvs.binaryValue());

                // advance() lands past a valueless doc the same way.
                BinaryDocValues advanced = columnarSortKeys(leaf, false);
                assertEquals(3, advanced.advance(1));
                assertEquals(new BytesRef("omega"), advanced.binaryValue());
            }
        }
    }

    /**
     * The sort key taken from a real column rather than from a payload. A column answers {@code extreme} itself,
     * over ordinals where it keeps a dictionary, so the sort key never goes through the payload the other tests
     * feed in - and it has to be the same key either way, for every shape a document takes.
     */
    public void testColumnarColumnAgreesWithThePayload() throws IOException {
        final String[][] docs = new String[between(200, 600)][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = switch (d % 7) {
                case 0 -> new String[] { "term-" + (d % 5) };
                case 1 -> new String[] { "term-" + (d % 5), "term-" + ((d + 3) % 5) };
                // A value the dictionary will not name, so the extreme is decided by bytes rather than by ordinal.
                case 2 -> new String[] { "escaped-" + d, "term-1" };
                case 3 -> new String[] { null, "term-2" };
                case 4 -> new String[] { null };
                case 5 -> new String[0];
                default -> null;
            };
        }
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(columnarCodec()))) {
            for (String[] slots : docs) {
                final LuceneDocument doc = new LuceneDocument();
                if (slots != null) {
                    if (slots.length == 0) {
                        ColumnarBinaryDocValuesField.recordEmptyArray(doc, "name");
                    }
                    for (String slot : slots) {
                        if (slot == null) {
                            ColumnarBinaryDocValuesField.recordNull(doc, "name");
                        } else {
                            ColumnarBinaryDocValuesField.recordValue(doc, "name", new BytesRef(slot), ValueOrdering.UNSORTED);
                        }
                    }
                }
                w.addDocument(doc);
            }
            w.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                final LeafReader leaf = getOnlyLeafReader(reader);
                assertThat(
                    "the field has to be a column, or this reads the same payload path as every other test here",
                    leaf.getBinaryDocValues("name"),
                    instanceOf(StringColumnSource.class)
                );
                for (boolean maxMode : new boolean[] { false, true }) {
                    final BinaryDocValues keys = columnarSortKeys(leaf, maxMode);
                    for (int d = 0; d < docs.length; d++) {
                        final BytesRef expected = expectedExtreme(docs[d], maxMode);
                        if (keys.advanceExact(d) == false) {
                            assertNull("doc " + d + " maxMode=" + maxMode + " read as missing", expected);
                            continue;
                        }
                        assertEquals("doc " + d + " maxMode=" + maxMode, expected, keys.binaryValue());
                    }
                }
            }
        }
    }

    /** The extreme non-null value of a document, worked out from the values themselves. */
    private static BytesRef expectedExtreme(String[] slots, boolean maxMode) {
        if (slots == null) {
            return null;
        }
        BytesRef best = null;
        for (String slot : slots) {
            if (slot == null) {
                continue;
            }
            final BytesRef candidate = new BytesRef(slot);
            if (best == null || (maxMode ? candidate.compareTo(best) > 0 : candidate.compareTo(best) < 0)) {
                best = candidate;
            }
        }
        return best;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        // The type is injected rather than read from a field attribute: these documents are built through
        // ColumnarBinaryDocValuesField, which is the mapper's field and carries no codec attribute.
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(
            (fieldName, fieldType) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
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

    private static BinaryDocValues columnarSortKeys(LeafReader leaf, boolean maxMode) throws IOException {
        return new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, maxMode, COLUMNAR_PAYLOAD).getSortKeyDocValues(
            leaf
        );
    }

    // =========================================================================
    // Provider round-trip serialization
    // =========================================================================

    public void testProviderRoundTrip_minMode_stringFirst() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, SortField.STRING_FIRST, false));
    }

    public void testProviderRoundTrip_maxMode_stringLast() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_LAST, true));
    }

    public void testProviderRoundTrip_minMode_nullMissingValue() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, null, false));
    }

    public void testProviderRoundTrip_maxMode_stringFirst() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_FIRST, true));
    }

    public void testProviderRoundTrip_minMode_reverseTrue() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_LAST, false));
    }

    public void testProviderRoundTrip_arrayOrderInlineNull() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, SortField.STRING_LAST, true, ARRAY_ORDER_INLINE_NULL));
    }

    public void testProviderRoundTrip_columnarPayload() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, SortField.STRING_LAST, true, COLUMNAR_PAYLOAD));
    }

    /**
     * The format is written into segment info as an ordinal, so where each constant sits is on-disk state and none of them may move.
     * The first two have to keep the {@code 0}/{@code 1} of the boolean they replaced, so segments written before the format became
     * three-valued read back as themselves. {@code COLUMNAR_PAYLOAD} carries no old segments yet, but it does from the release it
     * ships in, and pinning it now is what stops a fourth format being inserted above it. A new format goes on the end.
     */
    public void testProviderWireFormatOrdinalsAreStable() {
        assertEquals(0, SEPARATE_COUNT.ordinal());
        assertEquals(1, ARRAY_ORDER_INLINE_NULL.ordinal());
        assertEquals(2, COLUMNAR_PAYLOAD.ordinal());
    }

    private static void assertRoundTrip(MultiValuedBinaryDocValuesSortField original) throws IOException {
        var provider = new MultiValuedBinaryDocValuesSortField.Provider();
        var buf = new ByteBuffersDataOutput();
        provider.writeSortField(original, buf);
        MultiValuedBinaryDocValuesSortField restored = (MultiValuedBinaryDocValuesSortField) provider.readSortField(buf.toDataInput());
        assertEquals(original.getField(), restored.getField());
        assertEquals(original.getReverse(), restored.getReverse());
        assertEquals(original.getMissingValue(), restored.getMissingValue());
        assertEquals(original.isMaxMode(), restored.isMaxMode());
        assertEquals(original.binaryFormat(), restored.binaryFormat());
    }
}
