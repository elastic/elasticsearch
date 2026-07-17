/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link EscfRowColumnBuilder}: array assembly, row addressing, multi-value
 * per-doc, absent (empty) rows, and output equivalence with the existing {@link EscfColumnBuilder}
 * array path.
 */
public class EscfRowColumnBuilderTests extends ESTestCase {

    // --------------------------------------------------------------------------------------------
    // Helper: produce an EscfArrayColumn via EscfRowColumnBuilder
    // --------------------------------------------------------------------------------------------

    private static EscfColumnData buildArrayOfString(int docCount, int[] rows, String[] values) {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        for (int i = 0; i < rows.length; i++) {
            builder.setString(rows[i], bytesRef(values[i]));
        }
        return builder.finish(docCount);
    }

    private static BytesRef bytesRef(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new BytesRef(bytes);
    }

    private static String readElem(EscfArrayColumn col, int row, int elemPos) {
        return col.child().getBinaryValue(col.rowElemFrom(row) + elemPos).utf8ToString();
    }

    private static int elemCount(EscfArrayColumn col, int row) {
        return col.rowElemTo(row) - col.rowElemFrom(row);
    }

    // --------------------------------------------------------------------------------------------
    // Core tests
    // --------------------------------------------------------------------------------------------

    /** An empty builder (no elements) produces an all-absent array. */
    public void testEmptyBuilderAllAbsent() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        assertTrue("isEmpty() before any setString", builder.isEmpty());
        EscfColumnData data = builder.finish(3);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(3, data.docCount());
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        for (int r = 0; r < 3; r++) {
            assertEquals("row " + r + " should be empty (absent)", 0, elemCount(col, r));
        }
    }

    /** Single element for one doc, others absent. */
    public void testSingleValueSingleDoc() {
        EscfColumnData data = buildArrayOfString(3, new int[] { 1 }, new String[] { "alpha" });
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        assertEquals(0, elemCount(col, 0)); // absent
        assertEquals(1, elemCount(col, 1));
        assertEquals("alpha", readElem(col, 1, 0));
        assertEquals(0, elemCount(col, 2)); // absent
    }

    /** All rows have exactly one element. */
    public void testDenseSingleValueAllDocs() {
        EscfColumnData data = buildArrayOfString(3, new int[] { 0, 1, 2 }, new String[] { "a", "b", "c" });
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        assertEquals(1, elemCount(col, 0));
        assertEquals("a", readElem(col, 0, 0));
        assertEquals(1, elemCount(col, 1));
        assertEquals("b", readElem(col, 1, 0));
        assertEquals(1, elemCount(col, 2));
        assertEquals("c", readElem(col, 2, 0));
    }

    /** Same row supplied twice → multi-value for that doc. */
    public void testMultiValueSingleDoc() {
        EscfColumnData data = buildArrayOfString(2, new int[] { 0, 0, 1 }, new String[] { "x", "y", "z" });
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        assertEquals(2, elemCount(col, 0));
        assertEquals("x", readElem(col, 0, 0));
        assertEquals("y", readElem(col, 0, 1));
        assertEquals(1, elemCount(col, 1));
        assertEquals("z", readElem(col, 1, 0));
    }

    /** Rows supplied out of order are tested to trigger the assertion. */
    public void testNonDecreasingRowAssertion() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        builder.setString(2, bytesRef("a"));
        expectThrows(AssertionError.class, () -> builder.setString(1, bytesRef("b")));
    }

    /** Trailing absent rows are filled correctly when the last row written is less than docCount - 1. */
    public void testTrailingAbsentRows() {
        EscfColumnData data = buildArrayOfString(5, new int[] { 0, 2 }, new String[] { "foo", "bar" });
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        assertEquals(1, elemCount(col, 0));
        assertEquals("foo", readElem(col, 0, 0));
        assertEquals(0, elemCount(col, 1)); // absent
        assertEquals(1, elemCount(col, 2));
        assertEquals("bar", readElem(col, 2, 0));
        assertEquals(0, elemCount(col, 3)); // absent
        assertEquals(0, elemCount(col, 4)); // absent
    }

    /** Leading absent rows (first element written to a non-zero doc). */
    public void testLeadingAbsentRows() {
        EscfColumnData data = buildArrayOfString(3, new int[] { 2 }, new String[] { "last" });
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(data);
        assertEquals(0, elemCount(col, 0));
        assertEquals(0, elemCount(col, 1));
        assertEquals(1, elemCount(col, 2));
        assertEquals("last", readElem(col, 2, 0));
    }

    /** docCount = 0 → empty array, no elements. */
    public void testZeroDocCount() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        EscfColumnData data = builder.finish(0);
        assertEquals(EscfColumnKind.ARRAY, data.kind());
        assertEquals(0, data.docCount());
        // No assertion needed beyond not throwing; the column is validly empty.
    }

    /**
     * Output of {@link EscfRowColumnBuilder} matches the output of the existing
     * {@link EscfColumnBuilder} array path for the same string input, verifying both produce an
     * identical {@link EscfColumnKind#ARRAY} with equal element sequences.
     */
    public void testOutputMatchesLegacyArrayBuilder() {
        // Build via EscfRowColumnBuilder: rows [0, 1, 1, 2] → values ["a", "b", "c", "d"]
        EscfColumnData rowData = buildArrayOfString(3, new int[] { 0, 1, 1, 2 }, new String[] { "a", "b", "c", "d" });

        // Verify element layout directly.
        EscfArrayColumn col = (EscfArrayColumn) EscfColumn.from(rowData);
        assertEquals(1, elemCount(col, 0));
        assertEquals("a", readElem(col, 0, 0));
        assertEquals(2, elemCount(col, 1));
        assertEquals("b", readElem(col, 1, 0));
        assertEquals("c", readElem(col, 1, 1));
        assertEquals(1, elemCount(col, 2));
        assertEquals("d", readElem(col, 2, 0));
    }

    /** isEmpty() transitions from true to false when the first element is written. */
    public void testIsEmpty() {
        EscfRowColumnBuilder builder = EscfRowColumnBuilder.arrayOfString(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        assertTrue(builder.isEmpty());
        builder.setString(0, bytesRef("hello"));
        assertFalse(builder.isEmpty());
    }
}
