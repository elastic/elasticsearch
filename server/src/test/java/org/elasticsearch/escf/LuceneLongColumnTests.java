/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link LuceneLongColumn}: verifies that the row cursor, Lucene tuple cursor,
 * dense values cursor, and {@link LuceneLongColumn#slice} all produce correct results for dense
 * and sparse data — and that the {@link LuceneLongColumn#withFilter} filter bitset correctly
 * restricts which documents are emitted.
 */
public class LuceneLongColumnTests extends ESTestCase {

    private static final FieldType FIELD_TYPE;
    static {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.NUMERIC);
        ft.freeze();
        FIELD_TYPE = ft;
    }

    /** Builds a dense column where every doc has a value. */
    private static LuceneLongColumn buildDenseColumn(long... values) {
        byte[] data = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeLongLE(values[i], data, i * 8);
        }
        return LuceneLongColumn.longColumn(new BytesRef(data), "num", FIELD_TYPE, LongColumn.NumericKind.LONG);
    }

    /** Builds a sparse column with the given validity bitset. {@code presentDocs} are the set bits. */
    private static LuceneLongColumn buildSparseColumn(int docCount, int[] presentDocs, long[] values) {
        assert presentDocs.length == values.length;
        FixedBitSet validity = new FixedBitSet(docCount);
        byte[] data = new byte[docCount * 8]; // slots for every doc; absent slots are 0
        for (int i = 0; i < presentDocs.length; i++) {
            validity.set(presentDocs[i]);
            ByteUtils.writeLongLE(values[i], data, presentDocs[i] * 8);
        }
        return LuceneLongColumn.sparseLongColumn(data, validity, docCount, "num", FIELD_TYPE, LongColumn.NumericKind.LONG);
    }

    /** Drains a tuple cursor into a docId → value map. */
    private static Map<Integer, Long> drainTuples(LuceneLongColumn col) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        Column luceneCol = col.toLuceneColumn();
        LongTupleCursor cursor = ((LongColumn) luceneCol).tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            assertFalse("duplicate doc " + doc, result.containsKey(doc));
            result.put(doc, cursor.longValue());
        }
        return result;
    }

    /** Drains a row cursor into a docId → value map. */
    private static Map<Integer, Long> drainRowCursor(LuceneColumn.RowFieldCursor cursor) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            List<IndexableField> fields = new ArrayList<>();
            cursor.appendCurrentFields(fields);
            assertEquals("expected one field per doc", 1, fields.size());
            result.put(doc, fields.get(0).numericValue().longValue());
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Dense column — no filter
    // -------------------------------------------------------------------------

    public void testDenseColumnTuples() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L);
        Map<Integer, Long> result = drainTuples(col);
        assertEquals(Map.of(0, 10L, 1, 20L, 2, 30L), result);
    }

    public void testDenseColumnRowCursor() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L);
        Map<Integer, Long> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(Map.of(0, 10L, 1, 20L, 2, 30L), result);
    }

    public void testDenseColumnDensityIsDense() {
        LuceneLongColumn col = buildDenseColumn(1L, 2L);
        assertEquals(Column.Density.DENSE, col.density());
    }

    public void testDenseColumnValues() {
        LuceneLongColumn col = buildDenseColumn(7L, 8L, 9L);
        var cursor = col.values();
        assertEquals(7L, cursor.nextLong());
        assertEquals(8L, cursor.nextLong());
        assertEquals(9L, cursor.nextLong());
    }

    public void testDenseColumnSlice() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L, 40L, 50L);
        LuceneLongColumn sliced = col.slice(1, 3); // docs 1,2,3 → local 0,1,2
        Map<Integer, Long> result = drainTuples(sliced);
        assertEquals(Map.of(0, 20L, 1, 30L, 2, 40L), result);
    }

    public void testToLuceneColumnReturnsSelf() {
        LuceneLongColumn col = buildDenseColumn(1L);
        assertSame(col, col.toLuceneColumn());
    }

    // -------------------------------------------------------------------------
    // Sparse column — no filter
    // -------------------------------------------------------------------------

    public void testSparseColumnTuples() {
        // 5 docs; only docs 1 and 3 are present
        LuceneLongColumn col = buildSparseColumn(5, new int[] { 1, 3 }, new long[] { 100L, 300L });
        Map<Integer, Long> result = drainTuples(col);
        assertEquals(Map.of(1, 100L, 3, 300L), result);
    }

    public void testSparseColumnRowCursor() {
        LuceneLongColumn col = buildSparseColumn(5, new int[] { 1, 3 }, new long[] { 100L, 300L });
        Map<Integer, Long> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(Map.of(1, 100L, 3, 300L), result);
    }

    public void testSparseColumnDensityIsSparse() {
        LuceneLongColumn col = buildSparseColumn(3, new int[] { 0 }, new long[] { 5L });
        assertEquals(Column.Density.SPARSE, col.density());
    }

    public void testSparseColumnSlice() {
        // 6 docs; docs 0, 2, 4 have values
        LuceneLongColumn col = buildSparseColumn(6, new int[] { 0, 2, 4 }, new long[] { 10L, 20L, 40L });
        // Slice docs [1, 5) → original docs 1..4 become local 0..3; doc 2→local 1 and doc 4→local 3
        LuceneLongColumn sliced = col.slice(1, 4);
        Map<Integer, Long> result = drainTuples(sliced);
        assertEquals(Map.of(1, 20L, 3, 40L), result);
    }

    // -------------------------------------------------------------------------
    // Filter on dense column
    // -------------------------------------------------------------------------

    public void testFilterDenseColumnTuples() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L, 40L, 50L);
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(1);
        filter.set(3);
        LuceneLongColumn filtered = col.withFilter(filter);
        Map<Integer, Long> result = drainTuples(filtered);
        assertEquals(Map.of(1, 20L, 3, 40L), result);
    }

    public void testFilterDenseColumnRowCursor() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L, 40L, 50L);
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(4);
        LuceneLongColumn filtered = col.withFilter(filter);
        Map<Integer, Long> result = drainRowCursor(filtered.rowFieldCursor());
        assertEquals(Map.of(0, 10L, 4, 50L), result);
    }

    public void testFilterForcesSparseDensity() {
        LuceneLongColumn col = buildDenseColumn(1L, 2L, 3L);
        assertEquals(Column.Density.DENSE, col.density());
        FixedBitSet filter = new FixedBitSet(3);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        LuceneLongColumn filtered = col.withFilter(filter);
        assertEquals(Column.Density.SPARSE, filtered.density());
    }

    public void testFilterEmptyResultWhenNoDocsPass() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L);
        FixedBitSet filter = new FixedBitSet(3); // all bits clear
        LuceneLongColumn filtered = col.withFilter(filter);
        Map<Integer, Long> result = drainTuples(filtered);
        assertTrue("filter passes no docs → empty", result.isEmpty());
    }

    public void testFilterAllDocsPass() {
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L);
        FixedBitSet filter = new FixedBitSet(3);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        LuceneLongColumn filtered = col.withFilter(filter);
        Map<Integer, Long> result = drainTuples(filtered);
        assertEquals(Map.of(0, 10L, 1, 20L, 2, 30L), result);
    }

    public void testWithFilterNullIsNoOp() {
        // withFilter(null) on a column that already has a filter preserves the existing filter.
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L);
        FixedBitSet filter = new FixedBitSet(3);
        filter.set(1);
        LuceneLongColumn filtered = col.withFilter(filter);
        LuceneLongColumn stillFiltered = filtered.withFilter(null);
        assertEquals(drainTuples(filtered), drainTuples(stillFiltered));
    }

    public void testTuplesAndRowCursorAgreeWithFilter() {
        LuceneLongColumn col = buildDenseColumn(1L, 2L, 3L, 4L, 5L);
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(2);
        filter.set(4);
        LuceneLongColumn filtered = col.withFilter(filter);
        assertEquals(drainTuples(filtered), drainRowCursor(filtered.rowFieldCursor()));
    }

    // -------------------------------------------------------------------------
    // Filter on sparse column (filter intersects data validity)
    // -------------------------------------------------------------------------

    public void testFilterSparseColumnIntersectsValidity() {
        // Docs 1 and 3 have data; filter passes docs 0, 1, 2. Intersection: only doc 1.
        LuceneLongColumn col = buildSparseColumn(5, new int[] { 1, 3 }, new long[] { 100L, 300L });
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        LuceneLongColumn filtered = col.withFilter(filter);
        Map<Integer, Long> result = drainTuples(filtered);
        assertEquals(Map.of(1, 100L), result);
    }

    public void testFilterSparseColumnNoIntersection() {
        // Docs 1 and 3 have data; filter passes only doc 2 → empty result.
        LuceneLongColumn col = buildSparseColumn(5, new int[] { 1, 3 }, new long[] { 100L, 300L });
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(2);
        LuceneLongColumn filtered = col.withFilter(filter);
        assertTrue("no intersection → empty", drainTuples(filtered).isEmpty());
    }

    // -------------------------------------------------------------------------
    // Slice with active filter
    // -------------------------------------------------------------------------

    public void testSliceWindowsFilterCorrectly() {
        // 6 docs, filter passes docs {1, 3, 5}. slice(2, 4) → docs 2,3,4,5 become local 0,1,2,3.
        // Doc 1 is before the window; docs 3 and 5 land at local 1 and 3.
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L, 40L, 50L, 60L);
        FixedBitSet filter = new FixedBitSet(6);
        filter.set(1);
        filter.set(3);
        filter.set(5);
        LuceneLongColumn filtered = col.withFilter(filter);
        LuceneLongColumn sliced = filtered.slice(2, 4);
        Map<Integer, Long> result = drainTuples(sliced);
        assertEquals(Map.of(1, 40L, 3, 60L), result);
    }

    public void testSliceWithFilterThatCoversAllSliceDocsBecomesNull() {
        // Filter passes all 5 docs; windowing a slice where every bit in the window is set
        // means windowValidity returns null → sliced column has no filter → DENSE.
        LuceneLongColumn col = buildDenseColumn(10L, 20L, 30L, 40L, 50L);
        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        filter.set(3);
        filter.set(4);
        LuceneLongColumn filtered = col.withFilter(filter);
        // slice(1, 2) = docs 1,2 → both set in filter → windowed filter is all-set → null
        LuceneLongColumn sliced = filtered.slice(1, 2);
        assertEquals(Column.Density.DENSE, sliced.density());
        Map<Integer, Long> result = drainTuples(sliced);
        assertEquals(Map.of(0, 20L, 1, 30L), result);
    }

    public void testSliceFilterRowCursorMatchesTuples() {
        LuceneLongColumn col = buildDenseColumn(5L, 10L, 15L, 20L, 25L, 30L);
        FixedBitSet filter = new FixedBitSet(6);
        filter.set(1);
        filter.set(4);
        LuceneLongColumn sliced = col.withFilter(filter).slice(0, 6);
        assertEquals(drainTuples(sliced), drainRowCursor(sliced.rowFieldCursor()));
    }
}
