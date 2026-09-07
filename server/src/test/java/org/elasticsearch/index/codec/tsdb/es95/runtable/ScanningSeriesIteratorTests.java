/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link ScanningSeriesIterator}, which streams series from a dense {@code _tsid} sorted doc
 * values view: each {@link ScanningSeriesIterator#next()} scans forward to the next {@code _tsid} change and
 * reports that series' global ordinal (through the supplied remap), first doc, and doc count. Covers multi-doc
 * series, a single all-docs series, every-doc-distinct series, and that the local ordinal is remapped.
 */
public class ScanningSeriesIteratorTests extends ESTestCase {

    public void testStreamsSeriesBoundaries() throws IOException {
        final int[] perDocTsid = { 0, 0, 0, 1, 1, 2, 2, 2 };
        final SeriesIterator series = new ScanningSeriesIterator(dense(perDocTsid), LongValues.IDENTITY, perDocTsid.length);
        assertSeries(series, 0, 0, 3);
        assertSeries(series, 1, 3, 2);
        assertSeries(series, 2, 5, 3);
        assertFalse(series.next());
    }

    public void testSingleSeries() throws IOException {
        final int[] perDocTsid = { 4, 4, 4, 4 };
        final SeriesIterator series = new ScanningSeriesIterator(dense(perDocTsid), LongValues.IDENTITY, perDocTsid.length);
        assertSeries(series, 4, 0, 4);
        assertFalse(series.next());
    }

    public void testEveryDocDistinctSeries() throws IOException {
        final int[] perDocTsid = { 0, 1, 2, 3 };
        final SeriesIterator series = new ScanningSeriesIterator(dense(perDocTsid), LongValues.IDENTITY, perDocTsid.length);
        for (int i = 0; i < perDocTsid.length; i++) {
            assertSeries(series, i, i, 1);
        }
        assertFalse(series.next());
    }

    public void testRemapsLocalOrdinalToGlobal() throws IOException {
        final int[] perDocTsid = { 0, 0, 1, 2, 2 };
        final long[] global = { 10, 20, 30 };
        final SeriesIterator series = new ScanningSeriesIterator(dense(perDocTsid), remap(global), perDocTsid.length);
        assertSeries(series, 10, 0, 2);
        assertSeries(series, 20, 2, 1);
        assertSeries(series, 30, 3, 2);
        assertFalse(series.next());
    }

    private static void assertSeries(SeriesIterator series, long tsidOrd, int startDoc, int docCount) throws IOException {
        assertTrue(series.next());
        assertEquals("tsidOrd", tsidOrd, series.tsidOrd());
        assertEquals("startDoc", startDoc, series.startDoc());
        assertEquals("docCount", docCount, series.docCount());
    }

    private static LongValues remap(long[] values) {
        return new LongValues() {
            @Override
            public long get(long index) {
                return values[(int) index];
            }
        };
    }

    private static SortedDocValues dense(int[] perDocOrd) {
        int max = 0;
        for (final int ord : perDocOrd) {
            max = Math.max(max, ord);
        }
        final int valueCount = max + 1;
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public int ordValue() {
                return perDocOrd[doc];
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                doc = target >= perDocOrd.length ? NO_MORE_DOCS : target;
                return doc;
            }

            @Override
            public long cost() {
                return perDocOrd.length;
            }

            @Override
            public int getValueCount() {
                return valueCount;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
