/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.LongValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link RunTableSortedSetSeriesCursor}, the per-segment adapter that pairs a {@link SeriesIterator}
 * with a field's run-table {@link org.elasticsearch.index.codec.tsdb.SortedSetRunView} to produce the
 * {@link SortedSetSeriesCursor} the merger consumes. Covers a field run spanning several series, the per-ordinal
 * remap through the merged terms dictionary, and the absent (empty-set) series. Series are supplied through an
 * array-backed {@link SeriesIterator} double.
 */
public class RunTableSortedSetSeriesCursorTests extends ESTestCase {

    public void testFieldRunSpansMultipleSeriesWithRemap() throws IOException {
        // Field: run0 set {2,3} over docs [0,5), run1 set {5} over docs [5,6).
        final RunTableSortedSetOrdinalReader.Runs fieldRuns = runs(new int[] { 0, 5 }, new int[] { 0, 2, 3 }, new long[] { 2, 3, 5 }, 6);
        final SeriesIterator series = seriesIterator(new long[] { 10, 20, 30 }, new int[] { 0, 2, 5 }, new int[] { 2, 3, 1 });
        final LongValues remap = remap(new long[] { 0, 10, 20, 30, 40, 50 });
        final RunTableSortedSetSeriesCursor cursor = new RunTableSortedSetSeriesCursor(series, fieldRuns, remap);

        assertTrue(cursor.next());
        assertEquals(10, cursor.tsidOrd());
        assertEquals(2, cursor.docCount());
        assertEquals(2, cursor.ordCount());
        assertEquals(20, cursor.ordAt(0));
        assertEquals(30, cursor.ordAt(1));

        assertTrue(cursor.next());
        assertEquals(20, cursor.tsidOrd());
        assertEquals(3, cursor.docCount());
        assertEquals(2, cursor.ordCount());
        assertEquals(20, cursor.ordAt(0));
        assertEquals(30, cursor.ordAt(1));

        assertTrue(cursor.next());
        assertEquals(30, cursor.tsidOrd());
        assertEquals(1, cursor.docCount());
        assertEquals(1, cursor.ordCount());
        assertEquals(50, cursor.ordAt(0));

        assertFalse(cursor.next());
    }

    public void testEmptySetSeries() throws IOException {
        // Field: run0 {} over [0,2), run1 {0,1} over [2,5), run2 {} over [5,6).
        final RunTableSortedSetOrdinalReader.Runs fieldRuns = runs(new int[] { 0, 2, 5 }, new int[] { 0, 0, 2, 2 }, new long[] { 0, 1 }, 6);
        final SeriesIterator series = seriesIterator(new long[] { 10, 20, 30 }, new int[] { 0, 2, 5 }, new int[] { 2, 3, 1 });
        final LongValues remap = remap(new long[] { 0, 1 });
        final RunTableSortedSetSeriesCursor cursor = new RunTableSortedSetSeriesCursor(series, fieldRuns, remap);

        assertTrue(cursor.next());
        assertEquals(0, cursor.ordCount());

        assertTrue(cursor.next());
        assertEquals(2, cursor.ordCount());
        assertEquals(0, cursor.ordAt(0));
        assertEquals(1, cursor.ordAt(1));

        assertTrue(cursor.next());
        assertEquals(0, cursor.ordCount());

        assertFalse(cursor.next());
    }

    private static SeriesIterator seriesIterator(long[] tsidOrds, int[] startDocs, int[] docCounts) {
        return new SeriesIterator() {
            private int i = -1;

            @Override
            public boolean next() {
                return ++i < tsidOrds.length;
            }

            @Override
            public long tsidOrd() {
                return tsidOrds[i];
            }

            @Override
            public int startDoc() {
                return startDocs[i];
            }

            @Override
            public int docCount() {
                return docCounts[i];
            }
        };
    }

    private static RunTableSortedSetOrdinalReader.Runs runs(int[] startDocs, int[] setOffsets, long[] ordStream, int maxDoc) {
        return RunTableSortedSetOrdinalReader.openRuns(
            remap(toLong(startDocs)),
            remap(toLong(setOffsets)),
            remap(ordStream),
            startDocs.length,
            maxDoc
        );
    }

    private static long[] toLong(int[] values) {
        final long[] out = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i];
        }
        return out;
    }

    private static LongValues remap(long[] values) {
        return new LongValues() {
            @Override
            public long get(long index) {
                return values[(int) index];
            }
        };
    }
}
