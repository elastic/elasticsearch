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
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link LuceneBinaryColumn}: verifies that the row cursor, Lucene tuple cursor,
 * dense values cursor, and {@link LuceneBinaryColumn#slice} all produce correct results for both
 * the sparse array shape (multi-value docs, absent rows) and the dense string shape.
 */
public class LuceneBinaryColumnTests extends ESTestCase {

    private static LuceneBinaryColumn buildArrayColumn(int docCount, int[] docs, String[] values) {
        EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE, BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.hintScalar(EscfColumnKind.STRING); // an all-absent column still finishes as STRING
        for (int i = 0; i < docs.length; i++) {
            builder.setString(docs[i], new BytesRef(values[i]));
        }
        // Use of() so that single-valued output (STRING scalar) and multi-valued output (ARRAY) are
        // both handled correctly: sparse when absent docs exist, dense when all docs are present.
        return LuceneBinaryColumn.of(builder.finish(docCount), "_field_names", StringField.TYPE_NOT_STORED);
    }

    private static LuceneBinaryColumn buildStringColumn(String... values) {
        // Build raw offset + byte arrays for EscfColumnData.ofVarWidth.
        int[] offsets = new int[values.length + 1];
        int totalBytes = 0;
        for (String v : values) {
            totalBytes += new BytesRef(v).length;
        }
        byte[] data = new byte[totalBytes];
        int pos = 0;
        for (int i = 0; i < values.length; i++) {
            BytesRef ref = new BytesRef(values[i]);
            offsets[i] = pos;
            System.arraycopy(ref.bytes, ref.offset, data, pos, ref.length);
            pos += ref.length;
        }
        offsets[values.length] = pos;
        EscfColumnData columnData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, values.length, null, offsets, new BytesArray(data));
        return LuceneBinaryColumn.stringColumn(columnData, "content", StringField.TYPE_NOT_STORED);
    }

    private static Map<Integer, List<String>> drainRowCursor(LuceneColumn.RowFieldCursor cursor) {
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

    private static Map<Integer, List<String>> drainTupleCursor(LuceneBinaryColumn col) {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        Column luceneCol = col.toLuceneColumn();
        ObjectTupleCursor<BytesRef> cursor = ((BinaryColumn) luceneCol).tuples();
        int doc = cursor.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            result.computeIfAbsent(doc, k -> new ArrayList<>()).add(cursor.value().utf8ToString());
            doc = cursor.nextDoc();
        }
        return result;
    }

    /** Empty column yields NO_MORE_DOCS immediately. */
    public void testArrayRowCursorEmpty() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[0], new String[0]);
        LuceneColumn.RowFieldCursor cursor = col.rowFieldCursor();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
    }

    /** Single value in the middle; surrounding rows are skipped. */
    public void testArrayRowCursorSingleValue() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[] { 1 }, new String[] { "alpha" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(1, result.size());
        assertEquals(List.of("alpha"), result.get(1));
    }

    /** All rows present, each with one value. */
    public void testArrayRowCursorDense() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[] { 0, 1, 2 }, new String[] { "a", "b", "c" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("a"), result.get(0));
        assertEquals(List.of("b"), result.get(1));
        assertEquals(List.of("c"), result.get(2));
    }

    /** Multi-value doc: same doc-id returned for each element, all values collected. */
    public void testArrayRowCursorMultiValueDoc() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[] { 0, 0, 1, 2, 2, 2 }, new String[] { "x", "y", "only", "p", "q", "r" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("x", "y"), result.get(0));
        assertEquals(List.of("only"), result.get(1));
        assertEquals(List.of("p", "q", "r"), result.get(2));
    }

    /** Leading, mid, and trailing absent rows are silently skipped. */
    public void testArrayRowCursorAbsentRows() {
        // doc 0 absent, doc 1 has value, doc 2 absent, doc 3 has value, doc 4 absent
        LuceneBinaryColumn col = buildArrayColumn(5, new int[] { 1, 3 }, new String[] { "one", "three" });
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(2, result.size());
        assertEquals(List.of("one"), result.get(1));
        assertEquals(List.of("three"), result.get(3));
        assertNull("doc 0 is absent", result.get(0));
        assertNull("doc 2 is absent", result.get(2));
        assertNull("doc 4 is absent", result.get(4));
    }

    /** Tuple cursor produces NO_MORE_DOCS immediately for an empty column. */
    public void testArrayTupleCursorEmpty() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[0], new String[0]);
        Column luceneCol = col.toLuceneColumn();
        ObjectTupleCursor<BytesRef> cursor = ((BinaryColumn) luceneCol).tuples();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
    }

    /**
     * Tuple cursor results must be identical to row cursor results for all shapes
     * (single-value, multi-value, absent).
     */
    public void testArrayTupleCursorMatchesRowCursor() {
        LuceneBinaryColumn col = buildArrayColumn(4, new int[] { 0, 1, 1, 3 }, new String[] { "a", "b1", "b2", "d" });
        Map<Integer, List<String>> fromRow = drainRowCursor(col.rowFieldCursor());
        Map<Integer, List<String>> fromTuple = drainTupleCursor(col);
        assertEquals(fromRow, fromTuple);
    }

    /** Tuple cursor is SPARSE density (multi-value skips absent docs; same doc repeats). */
    public void testArrayTupleCursorSparseMultiValue() {
        LuceneBinaryColumn col = buildArrayColumn(3, new int[] { 0, 0, 2, 2 }, new String[] { "x", "y", "p", "q" });
        Map<Integer, List<String>> result = drainTupleCursor(col);
        assertEquals(2, result.size());
        assertEquals(List.of("x", "y"), result.get(0));
        assertEquals(List.of("p", "q"), result.get(2));
        assertNull("doc 1 is absent", result.get(1));
    }

    /**
     * {@link LuceneBinaryColumn#slice} returns a view over the requested document sub-range;
     * doc-ids are re-indexed relative to the slice start.
     */
    public void testArraySliceBasic() {
        // Full column: 5 docs. Slice docs [1, 3) → 2 docs.
        LuceneBinaryColumn col = buildArrayColumn(5, new int[] { 0, 1, 2, 3, 4 }, new String[] { "doc0", "doc1", "doc2", "doc3", "doc4" });
        LuceneBinaryColumn sliced = col.slice(1, 2);
        // Docs 1..2 (original) → docs 0..1 (slice-local).
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertEquals(2, result.size());
        assertEquals(List.of("doc1"), result.get(0));
        assertEquals(List.of("doc2"), result.get(1));
    }

    /** Slice of an absent region yields an all-absent column (empty drain result). */
    public void testArraySliceAllAbsent() {
        LuceneBinaryColumn col = buildArrayColumn(5, new int[] { 0, 4 }, new String[] { "first", "last" });
        // Slice docs [1, 4) — all absent in the full column.
        LuceneBinaryColumn sliced = col.slice(1, 3);
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertTrue("absent slice yields no entries", result.isEmpty());
    }

    /** Slice containing only a multi-value doc preserves all its elements. */
    public void testArraySliceMultiValueDoc() {
        LuceneBinaryColumn col = buildArrayColumn(4, new int[] { 0, 1, 1, 2 }, new String[] { "a", "b1", "b2", "c" });
        // slice(1, 1) → exactly the multi-value doc 1 from the full column.
        LuceneBinaryColumn sliced = col.slice(1, 1);
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertEquals(1, result.size());
        assertEquals(List.of("b1", "b2"), result.get(0));
    }

    /** Tuple cursor works correctly on a sliced column. */
    public void testArraySliceTupleCursorMatchesRowCursor() {
        LuceneBinaryColumn col = buildArrayColumn(6, new int[] { 0, 0, 2, 3, 3, 5 }, new String[] { "a0", "a1", "b", "c0", "c1", "d" });
        // Slice docs [2, 6) → original docs 2..5 become docs 0..3 in slice.
        LuceneBinaryColumn sliced = col.slice(2, 4);
        Map<Integer, List<String>> fromRow = drainRowCursor(sliced.rowFieldCursor());
        Map<Integer, List<String>> fromTuple = drainTupleCursor(sliced);
        assertEquals(fromRow, fromTuple);
    }

    /** Row cursor over a dense string column visits every row in order. */
    public void testStringRowCursorDense() {
        LuceneBinaryColumn col = buildStringColumn("alpha", "beta", "gamma");
        Map<Integer, List<String>> result = drainRowCursor(col.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("alpha"), result.get(0));
        assertEquals(List.of("beta"), result.get(1));
        assertEquals(List.of("gamma"), result.get(2));
    }

    /** Tuple cursor over a dense string column matches the row cursor. */
    public void testStringTupleCursorMatchesRowCursor() {
        LuceneBinaryColumn col = buildStringColumn("x", "y", "z", "w");
        Map<Integer, List<String>> fromRow = drainRowCursor(col.rowFieldCursor());
        Map<Integer, List<String>> fromTuple = drainTupleCursor(col);
        assertEquals(fromRow, fromTuple);
    }

    /** Dense values cursor ({@link LuceneBinaryColumn#values()}) returns all values in row order. */
    public void testStringValuesCursor() {
        LuceneBinaryColumn col = buildStringColumn("one", "two", "three");
        Column luceneCol = col.toLuceneColumn();
        BytesRefValuesCursor cursor = ((BinaryColumn) luceneCol).values();
        assertEquals("one", cursor.nextValue().utf8ToString());
        assertEquals("two", cursor.nextValue().utf8ToString());
        assertEquals("three", cursor.nextValue().utf8ToString());
    }

    /** Row cursor on a sliced dense column re-indexes doc-ids relative to the slice start. */
    public void testStringSliceRowCursor() {
        LuceneBinaryColumn col = buildStringColumn("a", "b", "c", "d", "e");
        // Slice docs [2, 5) → docs 2..4 (original) become 0..2 in slice.
        LuceneBinaryColumn sliced = col.slice(2, 3);
        Map<Integer, List<String>> result = drainRowCursor(sliced.rowFieldCursor());
        assertEquals(3, result.size());
        assertEquals(List.of("c"), result.get(0));
        assertEquals(List.of("d"), result.get(1));
        assertEquals(List.of("e"), result.get(2));
    }

    /** {@code toLuceneColumn()} returns {@code this} (the adaptor is itself a Lucene Column). */
    public void testToLuceneColumnReturnsSelf() {
        LuceneBinaryColumn col = buildStringColumn("only");
        assertSame(col, col.toLuceneColumn());
    }

    public void testRowFieldCursorValuesSurviveAdvance() {
        // Dense STRING column — exercises the BytesRefTupleCursor path in AbstractVarColumn.
        {
            LuceneBinaryColumn col = buildStringColumn("alpha", "beta", "gamma");
            LuceneColumn.RowFieldCursor cursor = col.rowFieldCursor();
            List<IndexableField> fields = new ArrayList<>();

            // Collect all fields before reading any, mirroring RowCursor.advance() behaviour.
            int doc;
            while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                cursor.appendCurrentFields(fields);
                cursor.nextDoc(); // advance past the field before we read it
                break;           // only need to show the first field survives one advance
            }
            // The BytesRef stored in the first field must still equal "alpha" even after nextDoc().
            assertFalse("expected at least one field", fields.isEmpty());
            assertEquals("alpha", fields.get(0).binaryValue().utf8ToString());
        }

        // ARRAY column — exercises the EscfArrayColumn.bytesRefCursor() path.
        {
            // 2 docs: doc 0 has ["hello", "world"], doc 1 has ["!"]
            LuceneBinaryColumn col = buildArrayColumn(2, new int[] { 0, 0, 1 }, new String[] { "hello", "world", "!" });
            LuceneColumn.RowFieldCursor cursor = col.rowFieldCursor();

            // Mirror MappedColumns.RowCursor.advance(): appendCurrentFields then nextDoc, alternating.
            // doc 0, first element "hello"
            assertEquals(0, cursor.nextDoc());
            List<IndexableField> doc0fields = new ArrayList<>();
            cursor.appendCurrentFields(doc0fields);  // appends Field("hello")
            // advance to second element of doc 0 — must not corrupt the "hello" Field
            assertEquals(0, cursor.nextDoc());
            cursor.appendCurrentFields(doc0fields);  // appends Field("world")
            // advance to doc 1 — must not corrupt either doc 0 Field
            assertEquals(1, cursor.nextDoc());

            assertEquals(2, doc0fields.size());
            assertEquals("hello", doc0fields.get(0).binaryValue().utf8ToString());
            assertEquals("world", doc0fields.get(1).binaryValue().utf8ToString());
        }
    }
}
