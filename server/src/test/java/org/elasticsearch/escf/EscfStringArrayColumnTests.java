/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.StringField;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.SliceableColumn;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link EscfStringArrayColumn}: verifies that the row cursor, Lucene tuple cursor,
 * and {@link EscfStringArrayColumn#slice} all produce correct results for multi-value docs, absent
 * rows, and contiguous sub-windows.
 */
public class EscfStringArrayColumnTests extends ESTestCase {

    // --------------------------------------------------------------------------------------------
    // Builder helpers
    // --------------------------------------------------------------------------------------------

    /** Build an EscfStringArrayColumn directly from (doc,value) pairs (non-decreasing docs). */
    private static EscfStringArrayColumn buildColumn(int docCount, int[] docs, String[] values) {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        for (int i = 0; i < docs.length; i++) {
            builder.setString(docs[i], new BytesRef(values[i]));
        }
        return EscfStringArrayColumn.fieldNamesColumn(builder.finish(docCount), "_field_names", StringField.TYPE_NOT_STORED);
    }

    /**
     * Drains the row cursor into a map of {@code docId → [values]}, collecting all elements.
     * The drain loop mirrors {@link org.elasticsearch.sourcebatch.MappedColumns.RowCursor#advance}:
     * prime once, then for each doc collect while the cursor keeps returning the same doc.
     */
    private static Map<Integer, List<String>> drainRowCursor(SliceableColumn.RowFieldCursor cursor) {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        int doc = cursor.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            List<IndexableField> fields = new ArrayList<>();
            cursor.appendCurrentFields(fields);
            result.computeIfAbsent(doc, k -> new ArrayList<>()).add(fields.get(0).binaryValue().utf8ToString());
            doc = cursor.nextDoc();
        }
        return result;
    }

    /**
     * Drains the tuple cursor (from {@code toLuceneColumn().tuples()}) into a map of
     * {@code docId → [values]}, so the same assertions can be applied as for the row cursor.
     */
    private static Map<Integer, List<String>> drainTupleCursor(EscfStringArrayColumn col) {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        Column luceneCol = col.toLuceneColumn();
        ObjectTupleCursor<BytesRef> cursor = ((org.apache.lucene.document.column.BinaryColumn) luceneCol).tuples();
        int doc = cursor.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            result.computeIfAbsent(doc, k -> new ArrayList<>()).add(cursor.value().utf8ToString());
            doc = cursor.nextDoc();
        }
        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Row cursor tests
    // --------------------------------------------------------------------------------------------

    /** Empty column yields NO_MORE_DOCS immediately. */
    public void testRowCursorEmpty() {
        EscfStringArrayColumn col = buildColumn(3, new int[0], new String[0]);
        SliceableColumn.RowFieldCursor cursor = col.rowFieldCursor();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
    }

    /** Single value in the middle; surrounding rows are skipped. */
    public void testRowCursorSingleValue() {
        EscfStringArrayColumn col = buildColumn(3, new int[] { 1 }, new String[] { "alpha" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(1, result.size());
        assertEquals(List.of("alpha"), result.get(1));
    }

    /** All rows dense, each with one value. */
    public void testRowCursorDense() {
        EscfStringArrayColumn col = buildColumn(3, new int[] { 0, 1, 2 }, new String[] { "a", "b", "c" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("a"), result.get(0));
        assertEquals(List.of("b"), result.get(1));
        assertEquals(List.of("c"), result.get(2));
    }

    /** Multi-value doc: same doc-id returned twice, both values collected. */
    public void testRowCursorMultiValueDoc() {
        EscfStringArrayColumn col = buildColumn(3, new int[] { 0, 0, 1, 2, 2, 2 }, new String[] { "x", "y", "only", "p", "q", "r" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("x", "y"), result.get(0));
        assertEquals(List.of("only"), result.get(1));
        assertEquals(List.of("p", "q", "r"), result.get(2));
    }

    /** Leading, mid, and trailing absent rows are silently skipped. */
    public void testRowCursorAbsentRows() {
        // doc 0 absent, doc 1 has value, doc 2 absent, doc 3 has value, doc 4 absent
        EscfStringArrayColumn col = buildColumn(5, new int[] { 1, 3 }, new String[] { "one", "three" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(2, result.size());
        assertEquals(List.of("one"), result.get(1));
        assertEquals(List.of("three"), result.get(3));
        assertNull("doc 0 is absent", result.get(0));
        assertNull("doc 2 is absent", result.get(2));
        assertNull("doc 4 is absent", result.get(4));
    }

    // --------------------------------------------------------------------------------------------
    // Tuple cursor (toLuceneColumn) tests — must match row cursor results exactly
    // --------------------------------------------------------------------------------------------

    /** Tuple cursor produces NO_MORE_DOCS immediately for an empty column. */
    public void testTupleCursorEmpty() {
        EscfStringArrayColumn col = buildColumn(3, new int[0], new String[0]);
        Column luceneCol = col.toLuceneColumn();
        ObjectTupleCursor<BytesRef> cursor = ((org.apache.lucene.document.column.BinaryColumn) luceneCol).tuples();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
    }

    /**
     * Tuple cursor results must be identical to row cursor results for all shapes
     * (single-value, multi-value, absent).
     */
    public void testTupleCursorMatchesRowCursor() {
        EscfStringArrayColumn col = buildColumn(4, new int[] { 0, 1, 1, 3 }, new String[] { "a", "b1", "b2", "d" });
        Map<Integer, List<String>> fromRow = drainRowCursor(col.rowFieldCursor());
        Map<Integer, List<String>> fromTuple = drainTupleCursor(col);
        assertEquals(fromRow, fromTuple);
    }

    /** Tuple cursor is SPARSE density (multi-value skips absent docs; same doc repeats). */
    public void testTupleCursorSparseMultiValue() {
        EscfStringArrayColumn col = buildColumn(3, new int[] { 0, 0, 2, 2 }, new String[] { "x", "y", "p", "q" });
        Map<Integer, List<String>> result = drainTupleCursor(col);
        assertEquals(2, result.size());
        assertEquals(List.of("x", "y"), result.get(0));
        assertEquals(List.of("p", "q"), result.get(2));
        assertNull("doc 1 is absent", result.get(1));
    }

    // --------------------------------------------------------------------------------------------
    // slice() tests
    // --------------------------------------------------------------------------------------------

    /**
     * {@link EscfStringArrayColumn#slice} returns a view over the requested document sub-range;
     * doc-ids are re-indexed relative to the slice start.
     */
    public void testSliceBasic() {
        // Full column: 5 docs. Slice docs [1, 3) → 2 docs (doc 1 and 2 in the full, doc 0 and 1 in slice).
        EscfStringArrayColumn col = buildColumn(5, new int[] { 0, 1, 2, 3, 4 }, new String[] { "doc0", "doc1", "doc2", "doc3", "doc4" });
        SliceableColumn sliced = col.slice(1, 2);
        // The sliced column wraps docs 1..2 (original) as docs 0..1 (slice-local).
        EscfStringArrayColumn slicedTyped = (EscfStringArrayColumn) sliced;
        Map<Integer, List<String>> result = drainRowCursor(slicedTyped.rowFieldCursor());
        assertEquals(2, result.size());
        assertEquals(List.of("doc1"), result.get(0));
        assertEquals(List.of("doc2"), result.get(1));
    }

    /** Slice of an absent region yields an all-absent column (empty drain result). */
    public void testSliceAllAbsent() {
        EscfStringArrayColumn col = buildColumn(5, new int[] { 0, 4 }, new String[] { "first", "last" });
        // Slice docs [1, 3) — both are absent in the full column.
        EscfStringArrayColumn sliced = (EscfStringArrayColumn) col.slice(1, 3);
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertTrue("absent slice yields no entries", result.isEmpty());
    }

    /** Slice containing only a multi-value doc preserves all its elements. */
    public void testSliceMultiValueDoc() {
        EscfStringArrayColumn col = buildColumn(4, new int[] { 0, 1, 1, 2 }, new String[] { "a", "b1", "b2", "c" });
        // slice(1, 1) → exactly one doc starting at doc 1 in the full column; that doc is multi-value.
        EscfStringArrayColumn sliced = (EscfStringArrayColumn) col.slice(1, 1);
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertEquals(1, result.size());
        assertEquals(List.of("b1", "b2"), result.get(0));
    }

    /** Tuple cursor works correctly on a sliced column. */
    public void testSliceTupleCursorMatchesRowCursor() {
        EscfStringArrayColumn col = buildColumn(6, new int[] { 0, 0, 2, 3, 3, 5 }, new String[] { "a0", "a1", "b", "c0", "c1", "d" });
        // Slice docs [2, 4) → original docs 2, 3 become docs 0, 1 in slice.
        EscfStringArrayColumn sliced = (EscfStringArrayColumn) col.slice(2, 4);
        Map<Integer, List<String>> fromRow = drainRowCursor(sliced.rowFieldCursor());
        Map<Integer, List<String>> fromTuple = drainTupleCursor(sliced);
        assertEquals(fromRow, fromTuple);
    }
}
