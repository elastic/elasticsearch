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
 * Unit tests for {@link RunTableSortedSeriesCursor}, the per-segment adapter that pairs a {@link SeriesIterator}
 * with a field's run-table {@link org.elasticsearch.index.codec.tsdb.SortedRunView} to produce the
 * {@link SortedSeriesCursor} the merger consumes. Covers a field run spanning several series, the field-ordinal
 * remap through the merged terms dictionary, and the absent (sentinel) series mapping to the merged sentinel
 * rather than through the ordinal map. Series are supplied through an array-backed {@link SeriesIterator} double.
 */
public class RunTableSortedSeriesCursorTests extends ESTestCase {

    public void testFieldRunSpansMultipleSeriesWithRemap() throws IOException {
        // Field: run0 ord 5 over docs [0,5), run1 ord 7 over docs [5,6).
        final RunTableSortedOrdinalReader.Runs fieldRuns = runs(new int[] { 0, 5 }, new long[] { 5, 7 }, 6, 8);
        // Three series; the first two both fall inside field run0.
        final SeriesIterator series = seriesIterator(new long[] { 10, 20, 30 }, new int[] { 0, 2, 5 }, new int[] { 2, 3, 1 });
        final LongValues remap = remap(new long[] { 0, 10, 20, 30, 40, 50, 60, 70 });
        final RunTableSortedSeriesCursor cursor = new RunTableSortedSeriesCursor(series, fieldRuns, remap, 8);

        assertTrue(cursor.next());
        assertEquals(10, cursor.tsidOrd());
        assertEquals(2, cursor.docCount());
        assertEquals(50, cursor.fieldOrd());

        assertTrue(cursor.next());
        assertEquals(20, cursor.tsidOrd());
        assertEquals(3, cursor.docCount());
        assertEquals(50, cursor.fieldOrd());

        assertTrue(cursor.next());
        assertEquals(30, cursor.tsidOrd());
        assertEquals(1, cursor.docCount());
        assertEquals(70, cursor.fieldOrd());

        assertFalse(cursor.next());
    }

    public void testAbsentSeriesEmitsMergedSentinel() throws IOException {
        // Field valueCount 3 -> source sentinel 3. run1 (ord 3) is an absent run over docs [2,4).
        final RunTableSortedOrdinalReader.Runs fieldRuns = runs(new int[] { 0, 2, 4 }, new long[] { 1, 3, 0 }, 5, 3);
        final SeriesIterator series = seriesIterator(new long[] { 10, 20, 30 }, new int[] { 0, 2, 4 }, new int[] { 2, 2, 1 });
        final LongValues remap = remap(new long[] { 0, 1, 2 });
        final int mergedSentinel = 9;
        final RunTableSortedSeriesCursor cursor = new RunTableSortedSeriesCursor(series, fieldRuns, remap, mergedSentinel);

        assertTrue(cursor.next());
        assertEquals(1, cursor.fieldOrd());

        assertTrue(cursor.next());
        assertEquals(mergedSentinel, cursor.fieldOrd());

        assertTrue(cursor.next());
        assertEquals(0, cursor.fieldOrd());

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

    private static RunTableSortedOrdinalReader.Runs runs(int[] startDocs, long[] ords, int maxDoc, int valueCount) {
        final long[] startDocsLong = new long[startDocs.length];
        for (int i = 0; i < startDocs.length; i++) {
            startDocsLong[i] = startDocs[i];
        }
        return RunTableSortedOrdinalReader.openRuns(remap(startDocsLong), remap(ords), startDocs.length, maxDoc, valueCount);
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
