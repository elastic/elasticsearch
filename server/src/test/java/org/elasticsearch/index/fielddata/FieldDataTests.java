/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FieldDataTests extends ESTestCase {

    private static class DummyValues extends LongValues {

        private final long value;

        DummyValues(long value) {
            this.value = value;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return true;
        }

        @Override
        public long longValue() throws IOException {
            return value;
        }
    }

    // These tests do not exercise the doc id iterator, so build the singleton without one.
    private static SortedNumericLongValues singletonLongsWithoutDocIdIterator(LongValues values) {
        return SortedNumericLongValues.singleton(values, null);
    }

    public void testSortableLongBitsToDoubles() throws IOException {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        LongValues values = new DummyValues(valueBits);

        SortedNumericDoubleValues asMultiDoubles = FieldData.sortableLongBitsToDoubles(singletonLongsWithoutDocIdIterator(values));
        DoubleValues asDoubles = SortedNumericDoubleValues.unwrapSingleton(asMultiDoubles);
        assertNotNull(asDoubles);
        assertTrue(asDoubles.advanceExact(0));
        assertEquals(value, asDoubles.doubleValue(), 0);

        values = new DummyValues(valueBits);
        asMultiDoubles = FieldData.sortableLongBitsToDoubles(singletonLongsWithoutDocIdIterator(values));
        LongValues backToLongs = SortedNumericLongValues.unwrapSingleton(FieldData.toSortableLongBits(asMultiDoubles));
        assertSame(values, backToLongs);

        SortedNumericLongValues multiValues = new SortedNumericLongValues(null) {

            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public long nextValue() {
                return valueBits;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };

        asMultiDoubles = FieldData.sortableLongBitsToDoubles(multiValues);
        assertEquals(value, asMultiDoubles.nextValue(), 0);
        assertSame(multiValues, FieldData.toSortableLongBits(asMultiDoubles));
    }

    public void testCastToDoubleSingleton() throws IOException {
        final long value = randomLong();
        LongValues values = new DummyValues(value);
        SortedNumericDoubleValues asMultiDoubles = FieldData.castToDouble(singletonLongsWithoutDocIdIterator(values));
        // a singleton input should produce a singleton output, not just the right values
        DoubleValues asDoubles = SortedNumericDoubleValues.unwrapSingleton(asMultiDoubles);
        assertNotNull(asDoubles);
        assertTrue(asDoubles.advanceExact(0));
        assertEquals((double) value, asDoubles.doubleValue(), 0);
    }

    public void testCastToDoubleNonSingleton() throws IOException {
        final long value = randomLong();
        SortedNumericLongValues multiValues = new SortedNumericLongValues(null) {
            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public long nextValue() {
                return value;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
        SortedNumericDoubleValues asMultiDoubles = FieldData.castToDouble(multiValues);
        // a genuinely multi-valued input should not be collapsed into a singleton
        assertNull(SortedNumericDoubleValues.unwrapSingleton(asMultiDoubles));
        assertTrue(asMultiDoubles.advanceExact(0));
        assertEquals((double) value, asMultiDoubles.nextValue(), 0);
    }

    /**
     * The singleton fast path must hand back the very same {@link SortedNumericDoubleValues#docIdIterator()}
     * as the wrapped values.
     */
    public void testCastToDoublePreservesDocIdIteratorSingleton() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues singleton = SortedNumericLongValues.singleton(new DummyValues(randomLong()), iterator);
        SortedNumericDoubleValues asMultiDoubles = FieldData.castToDouble(singleton);
        assertNotNull(SortedNumericDoubleValues.unwrapSingleton(asMultiDoubles));
        assertSame(iterator, asMultiDoubles.docIdIterator());
    }

    /**
     * The non-singleton path wraps the values rather than unwrapping them, and must also hand back
     * the very same iterator as the wrapped values.
     */
    public void testCastToDoublePreservesDocIdIteratorNonSingleton() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues multiValues = new SortedNumericLongValues(iterator) {
            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public long nextValue() {
                return 0L;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
        SortedNumericDoubleValues asMultiDoubles = FieldData.castToDouble(multiValues);
        assertNull(SortedNumericDoubleValues.unwrapSingleton(asMultiDoubles));
        assertSame(iterator, asMultiDoubles.docIdIterator());
    }

    /**
     * The iterator exposed by the singleton fast path must be the one that backs the values, so that positioning it
     * with {@code advance}/{@code nextDoc} makes {@code nextValue()} return that doc's value, and interleaving a
     * forward-moving {@code advanceExact} keeps both in sync. This uses real, sparse Lucene doc values.
     */
    public void testCastToDoubleDocIdIteratorIsInSyncWithValues() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            // docs 0, 2 and 4 have a value; docs 1 and 3 do not, which keeps the field sparse
            for (int doc = 0; doc < 5; doc++) {
                Document document = new Document();
                if (doc % 2 == 0) {
                    document.add(new SortedNumericDocValuesField("field", 10L * (doc + 1)));
                }
                w.addDocument(document);
            }
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                SortedNumericLongValues longs = SortedNumericLongValues.getLongValues("field", leaf);
                SortedNumericDoubleValues doubles = FieldData.castToDouble(longs);
                assertNotNull(SortedNumericDoubleValues.unwrapSingleton(doubles));

                DocIdSetIterator iterator = doubles.docIdIterator();
                assertNotNull(iterator);
                assertSame("the same iterator instance is returned on every call", iterator, doubles.docIdIterator());
                assertSame("the iterator is the one that backs the long values", longs.docIdIterator(), iterator);

                assertEquals(0, iterator.nextDoc());
                assertEquals(10d, doubles.nextValue(), 0);

                assertEquals(2, iterator.advance(1));
                assertEquals(30d, doubles.nextValue(), 0);

                // advanceExact on the same instance moves the shared iterator forward
                assertTrue(doubles.advanceExact(4));
                assertEquals(4, iterator.docID());
                assertEquals(50d, doubles.nextValue(), 0);

                assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
            }
        }
    }

    /**
     * {@link FieldData#sortableLongBitsToDoubles} on a singleton that is not already a converted view
     * wraps it, and must hand back the very same {@link SortedNumericDoubleValues#docIdIterator()}.
     */
    public void testSortableLongBitsToDoublesPreservesDocIdIteratorSingleton() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues singleton = SortedNumericLongValues.singleton(new DummyValues(randomLong()), iterator);
        SortedNumericDoubleValues asDoubles = FieldData.sortableLongBitsToDoubles(singleton);
        assertNotNull(SortedNumericDoubleValues.unwrapSingleton(asDoubles));
        assertSame(iterator, asDoubles.docIdIterator());
    }

    /**
     * {@link FieldData#sortableLongBitsToDoubles} on a singleton that came from
     * {@link FieldData#toSortableLongBits} unwraps the original doubles, and must still hand back the same iterator.
     */
    public void testSortableLongBitsToDoublesPreservesDocIdIteratorSingletonRoundTrip() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(singletonDoubles(iterator));
        SortedNumericDoubleValues asDoubles = FieldData.sortableLongBitsToDoubles(asLongs);
        assertNotNull(SortedNumericDoubleValues.unwrapSingleton(asDoubles));
        assertSame(iterator, asDoubles.docIdIterator());
    }

    /**
     * {@link FieldData#sortableLongBitsToDoubles} on multi-valued input that is not already a converted view
     * wraps it, and must hand back the very same iterator.
     */
    public void testSortableLongBitsToDoublesPreservesDocIdIteratorMultiValued() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericDoubleValues asDoubles = FieldData.sortableLongBitsToDoubles(multiLongs(iterator));
        assertNull(SortedNumericDoubleValues.unwrapSingleton(asDoubles));
        assertSame(iterator, asDoubles.docIdIterator());
    }

    /**
     * {@link FieldData#sortableLongBitsToDoubles} on multi-valued input that came from
     * {@link FieldData#toSortableLongBits} unwraps the original doubles, and must still hand back the same iterator.
     */
    public void testSortableLongBitsToDoublesPreservesDocIdIteratorMultiValuedRoundTrip() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(multiDoubles(iterator));
        SortedNumericDoubleValues asDoubles = FieldData.sortableLongBitsToDoubles(asLongs);
        assertNull(SortedNumericDoubleValues.unwrapSingleton(asDoubles));
        assertSame(iterator, asDoubles.docIdIterator());
    }

    /**
     * {@link FieldData#toSortableLongBits} on a singleton that is not already a converted view
     * wraps it, and must hand back the very same {@link SortedNumericLongValues#docIdIterator()}.
     */
    public void testToSortableLongBitsPreservesDocIdIteratorSingleton() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(singletonDoubles(iterator));
        assertNotNull(SortedNumericLongValues.unwrapSingleton(asLongs));
        assertSame(iterator, asLongs.docIdIterator());
    }

    /**
     * {@link FieldData#toSortableLongBits} on a singleton that came from {@link FieldData#sortableLongBitsToDoubles}
     * unwraps the original longs, and must still hand back the same iterator.
     */
    public void testToSortableLongBitsPreservesDocIdIteratorSingletonRoundTrip() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues original = SortedNumericLongValues.singleton(new DummyValues(randomLong()), iterator);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(FieldData.sortableLongBitsToDoubles(original));
        assertNotNull(SortedNumericLongValues.unwrapSingleton(asLongs));
        assertSame(iterator, asLongs.docIdIterator());
    }

    /**
     * {@link FieldData#toSortableLongBits} on multi-valued input that is not already a converted view
     * wraps it, and must hand back the very same iterator.
     */
    public void testToSortableLongBitsPreservesDocIdIteratorMultiValued() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(multiDoubles(iterator));
        assertNull(SortedNumericLongValues.unwrapSingleton(asLongs));
        assertSame(iterator, asLongs.docIdIterator());
    }

    /**
     * {@link FieldData#toSortableLongBits} on multi-valued input that came from
     * {@link FieldData#sortableLongBitsToDoubles} unwraps the original longs, and must still hand back the same iterator.
     */
    public void testToSortableLongBitsPreservesDocIdIteratorMultiValuedRoundTrip() {
        final DocIdSetIterator iterator = DocIdSetIterator.all(1);
        SortedNumericLongValues asLongs = FieldData.toSortableLongBits(FieldData.sortableLongBitsToDoubles(multiLongs(iterator)));
        assertNull(SortedNumericLongValues.unwrapSingleton(asLongs));
        assertSame(iterator, asLongs.docIdIterator());
    }

    /**
     * The empty doubles have no documents, so the iterator they expose must be empty too, rather than missing.
     */
    public void testEmptySortedNumericDoublesHasEmptyDocIdIterator() throws IOException {
        SortedNumericDoubleValues empty = FieldData.emptySortedNumericDoubles();
        DocIdSetIterator iterator = empty.docIdIterator();
        assertNotNull(iterator);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertFalse(empty.advanceExact(0));
    }

    public void testDoublesToSortableLongBits() throws IOException {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        DoubleValues values = new DoubleValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return true;
            }

            @Override
            public double doubleValue() {
                return value;
            }
        };

        SortedNumericLongValues asMultiLongs = FieldData.toSortableLongBits(
            SortedNumericDoubleValues.singletonWithoutDocIdIterator(values)
        );
        LongValues asLongs = SortedNumericLongValues.unwrapSingleton(asMultiLongs);
        assertNotNull(asLongs);
        assertTrue(asLongs.advanceExact(0));
        assertEquals(valueBits, asLongs.longValue());

        SortedNumericDoubleValues multiValues = new SortedNumericDoubleValues(null) {
            @Override
            public double nextValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return true;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };

        asMultiLongs = FieldData.toSortableLongBits(multiValues);
        assertEquals(valueBits, asMultiLongs.nextValue());
        assertSame(multiValues, FieldData.sortableLongBitsToDoubles(asMultiLongs));
    }

    private static LongValues asNumericDocValues(Long... values) {
        return new LongValues() {

            int docID = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                docID = target;
                return target < values.length && values[target] != null;
            }

            @Override
            public long longValue() throws IOException {
                return values[docID];
            }
        };
    }

    public void testReplaceMissingLongs() throws IOException {
        final LongValues values = asNumericDocValues(null, 3L, 2L, null, 5L, null);
        final LongValues replaced = FieldData.replaceMissing(values, 4);

        assertTrue(replaced.advanceExact(0));
        assertEquals(4L, replaced.longValue());

        assertTrue(replaced.advanceExact(1));
        assertEquals(3L, replaced.longValue());

        assertTrue(replaced.advanceExact(2));
        assertEquals(2L, replaced.longValue());

        assertTrue(replaced.advanceExact(3));
        assertEquals(4L, replaced.longValue());

        assertTrue(replaced.advanceExact(4));
        assertEquals(5L, replaced.longValue());

        assertTrue(replaced.advanceExact(5));
        assertEquals(4L, replaced.longValue());
    }

    private static DoubleValues asNumericDoubleValues(Double... values) {
        return new DoubleValues() {

            int docID = -1;

            @Override
            public boolean advanceExact(int target) throws IOException {
                docID = target;
                return target < values.length && values[target] != null;
            }

            @Override
            public double doubleValue() throws IOException {
                return values[docID];
            }
        };
    }

    public void testReplaceMissingDoubles() throws IOException {
        final DoubleValues values = asNumericDoubleValues(null, 1.3, 1.2, null, 1.5, null);
        final DoubleValues replaced = FieldData.replaceMissing(values, 1.4);

        assertTrue(replaced.advanceExact(0));
        assertEquals(1.4, replaced.doubleValue(), 0d);

        assertTrue(replaced.advanceExact(1));
        assertEquals(1.3, replaced.doubleValue(), 0d);

        assertTrue(replaced.advanceExact(2));
        assertEquals(1.2, replaced.doubleValue(), 0d);

        assertTrue(replaced.advanceExact(3));
        assertEquals(1.4, replaced.doubleValue(), 0d);

        assertTrue(replaced.advanceExact(4));
        assertEquals(1.5, replaced.doubleValue(), 0d);

        assertTrue(replaced.advanceExact(5));
        assertEquals(1.4, replaced.doubleValue(), 0d);
    }

    // A minimal genuinely multi-valued (non-singleton) long view that reports the given iterator.
    private static SortedNumericLongValues multiLongs(DocIdSetIterator iterator) {
        return new SortedNumericLongValues(iterator) {
            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public long nextValue() {
                return 0L;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }

    // A minimal genuinely multi-valued (non-singleton) double view that reports the given iterator.
    private static SortedNumericDoubleValues multiDoubles(DocIdSetIterator iterator) {
        return new SortedNumericDoubleValues(iterator) {
            @Override
            public boolean advanceExact(int target) {
                return true;
            }

            @Override
            public double nextValue() {
                return 0d;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };
    }

    // A minimal singleton double view that reports the given iterator.
    private static SortedNumericDoubleValues singletonDoubles(DocIdSetIterator iterator) {
        return SortedNumericDoubleValues.singleton(new DoubleValues() {
            @Override
            public boolean advanceExact(int doc) {
                return true;
            }

            @Override
            public double doubleValue() {
                return 0d;
            }
        }, iterator);
    }
}
