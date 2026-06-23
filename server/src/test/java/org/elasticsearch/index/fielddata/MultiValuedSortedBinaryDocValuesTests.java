/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public class MultiValuedSortedBinaryDocValuesTests extends ESTestCase {

    private static List<BytesRef> randomSortedUniqueBytesRefs(int count) {
        TreeSet<BytesRef> sorted = new TreeSet<>();
        while (sorted.size() < count) {
            sorted.add(new BytesRef(randomAlphanumericOfLength(10)));
        }
        return new ArrayList<>(sorted);
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads multi-valued binary doc values written by
     * {@link MultiValuedBinaryDocValuesField.SeparateCount}.
     */
    public void testReadValuesFromSeparateCount() throws IOException {
        // given
        List<BytesRef> expected = randomSortedUniqueBytesRefs(3);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                for (BytesRef value : expected) {
                    MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", value);
                }
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(expected.size(), values.docValueCount());
                List<BytesRef> actual = new ArrayList<>();
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                assertEquals(expected, actual);
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads multi-valued binary doc values written by
     * {@link MultiValuedBinaryDocValuesField.IntegratedCount}.
     */
    public void testReadValuesFromSeparateCountWithPreviousIndexVersion() throws IOException {
        // given
        List<BytesRef> expected = randomSortedUniqueBytesRefs(3);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(
                    IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES
                );
                for (BytesRef value : expected) {
                    MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                        doc,
                        "field",
                        value,
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                        previousVersion
                    );
                }
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(expected.size(), values.docValueCount());
                List<BytesRef> actual = new ArrayList<>();
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                assertEquals(expected, actual);
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads a single value written by
     * {@link MultiValuedBinaryDocValuesField.SeparateCount}.
     */
    public void testReadSingleValueFromSeparateCount() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", expected);
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads a single value written by
     * {@link MultiValuedBinaryDocValuesField.IntegratedCount}.
     */
    public void testReadSingleValueFromSeparateCountWithPreviousIndexVersion() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(
                    IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES
                );
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                    doc,
                    "field",
                    expected,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                    previousVersion
                );
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }

    /**
     * Verifies that the PlainBinary reader correctly reads single-valued binary doc values
     * written as a plain {@link BinaryDocValuesField} via auto-detection.
     */
    public void testReadSingleValuedBinaryDocValues() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                doc.addWithKey("field", new BinaryDocValuesField("field", expected));
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.from(leafReader, "field");

                // then
                assertNotNull(values);
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
                assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
            }
        }
    }

    /**
     * Indexes one doc the way the in-order (columnar) write path will: the {@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}
     * binary field is added only when there is at least one non-null value, while the {@code .counts} companion (total slots, including
     * nulls) is always written. A {@code null} element in {@code slots} denotes a null slot.
     */
    private static void addArrayOrderDoc(LuceneDocument doc, String field, List<BytesRef> slots) {
        var binaryField = new MultiValuedBinaryDocValuesField.ArrayOrderInlineNull(field);
        for (BytesRef slot : slots) {
            if (slot == null) {
                binaryField.addNull();
            } else {
                binaryField.add(slot);
            }
        }
        if (binaryField.hasNonNullValue()) {
            doc.add(binaryField);
        }
        doc.add(NumericDocValuesField.indexedField(binaryField.countFieldName(), slots.size()));
    }

    /**
     * Round-trips the given document-order slots through a Lucene segment and reads them back via
     * {@link SortingArrayOrderBinaryDocValues#from}. Returns the non-null values sorted by {@link BytesRef#compareTo} (the
     * {@link SortedBinaryDocValues} contract), or {@code null} when the reader reports no value (all-null array or empty array).
     */
    private List<BytesRef> roundTripArrayOrder(List<BytesRef> slots) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                addArrayOrderDoc(doc, "field", slots);
                iw.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedBinaryDocValues values = SortingArrayOrderBinaryDocValues.from(leafReader, "field");
                if (values.advanceExact(0) == false) {
                    return null;
                }
                List<BytesRef> actual = new ArrayList<>(values.docValueCount());
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                return actual;
            }
        }
    }

    /**
     * fromArrayOrder sorts the values (the SortedBinaryDocValues contract) while keeping duplicates (no dedup); document order is lost.
     */
    public void testArrayOrderSortsAndKeepsDuplicates() throws IOException {
        List<BytesRef> slots = List.of(new BytesRef("b"), new BytesRef("a"), new BytesRef("a"), new BytesRef("c"));
        List<BytesRef> expectedSorted = List.of(new BytesRef("a"), new BytesRef("a"), new BytesRef("b"), new BytesRef("c"));
        assertEquals(expectedSorted, roundTripArrayOrder(slots));
    }

    /**
     * fromArrayOrder drops inline nulls and returns only the non-null values, sorted.
     */
    public void testArrayOrderDropsInlineNulls() throws IOException {
        List<BytesRef> slots = Arrays.asList(new BytesRef("b"), null, new BytesRef("a"));
        assertEquals(List.of(new BytesRef("a"), new BytesRef("b")), roundTripArrayOrder(slots));
    }

    /**
     * A single non-null value uses the raw-bytes fast path (count == 1, no length prefix).
     */
    public void testArrayOrderSingleValue() throws IOException {
        BytesRef value = new BytesRef(randomAlphanumericOfLength(10));
        assertEquals(List.of(value), roundTripArrayOrder(List.of(value)));
    }

    /**
     * An empty string is a real value (stored with a {@code len+1 == 1} prefix) and is distinct from a null (which writes no slot bytes).
     */
    public void testArrayOrderEmptyStringDistinctFromNull() throws IOException {
        assertEquals(List.of(new BytesRef("")), roundTripArrayOrder(List.of(new BytesRef(""))));
        List<BytesRef> slots = Arrays.asList(new BytesRef("a"), new BytesRef(""), null, new BytesRef("b"));
        // sorted: the empty string sorts first, then "a", then "b" (the null slot is dropped)
        assertEquals(List.of(new BytesRef(""), new BytesRef("a"), new BytesRef("b")), roundTripArrayOrder(slots));
    }

    /**
     * A lone null, an all-null array, and an empty array all write no binary blob; the reader reports no (non-null) value.
     */
    public void testArrayOrderNoNonNullValues() throws IOException {
        assertNull(roundTripArrayOrder(Arrays.asList((BytesRef) null)));        // lone null
        assertNull(roundTripArrayOrder(Arrays.asList(null, null)));             // all-null array
        assertNull(roundTripArrayOrder(List.of()));                             // empty array
    }

    /**
     * Regression test for the MIN/MAX-by-position assumption in {@link org.elasticsearch.search.MultiValueMode}: it reads the first/last
     * value expecting sorted input, so the in-order fielddata reader must sort. The unsorted doc below would yield MIN="c"/MAX="b" if it
     * did not.
     */
    public void testArrayOrderFeedsMultiValueModeMinMax() throws IOException {
        List<BytesRef> slots = Arrays.asList(new BytesRef("c"), null, new BytesRef("a"), new BytesRef("a"), new BytesRef("b"));
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                addArrayOrderDoc(doc, "field", slots);
                iw.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                for (var mode : List.of(org.elasticsearch.search.MultiValueMode.MIN, org.elasticsearch.search.MultiValueMode.MAX)) {
                    var picked = mode.select(SortingArrayOrderBinaryDocValues.from(leafReader, "field"), null);
                    assertTrue(picked.advanceExact(0));
                    assertEquals(new BytesRef(mode == org.elasticsearch.search.MultiValueMode.MIN ? "a" : "c"), picked.binaryValue());
                }
            }
        }
    }

    /**
     * Randomized round-trip across mixed values (including empty strings and lengths that cross the 1-byte vint boundary) and nulls.
     */
    public void testArrayOrderRandomRoundTrip() throws IOException {
        int slotCount = between(2, 12);
        List<BytesRef> slots = new ArrayList<>(slotCount);
        List<BytesRef> expectedNonNull = new ArrayList<>();
        for (int i = 0; i < slotCount; i++) {
            if (randomBoolean()) {
                slots.add(null);
            } else {
                // include occasional empty strings and lengths around the 127-byte vint boundary
                BytesRef value = new BytesRef(randomBoolean() ? "" : randomAlphanumericOfLength(between(1, 130)));
                slots.add(value);
                expectedNonNull.add(value);
            }
        }
        expectedNonNull.sort(null); // fromArrayOrder returns values sorted by BytesRef#compareTo
        List<BytesRef> actual = roundTripArrayOrder(slots);
        if (expectedNonNull.isEmpty()) {
            assertNull(actual);
        } else {
            assertEquals(expectedNonNull, actual);
        }
    }

    /**
     * Verifies that fromMultiValued() falls back to IntegratedCounts when .counts is absent.
     */
    public void testFromMultiValuedFallsBackToIntegratedCounts() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);

        // Write an IntegratedCount value using the old index version
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                    doc,
                    "field",
                    expected,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                    oldVersion
                );
                iw.addDocument(doc);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // IntegratedCounts reader should decode correctly
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }
}
