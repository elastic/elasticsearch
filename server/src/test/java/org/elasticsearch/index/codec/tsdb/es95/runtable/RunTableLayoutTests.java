/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Unit tests for the {@link AbstractRunTableLayout} selection predicate and for the round-trip
 * correctness of both {@link SortedRunTableLayout} and {@link SortedSetRunTableLayout}.
 */
public class RunTableLayoutTests extends ESTestCase {

    public void testFitsRunTableWhenRunsAreFewRelativeToMaxDoc() {
        // 2 runs over 8 docs: 2*2=4 <= 8, so the run table fits.
        assertTrue(fitsRunTable(2, 8));
    }

    public void testFitsRunTableAtBoundary() {
        // 4 runs over 8 docs: 4*2=8 which is not > 8, so the run table fits at the boundary.
        assertTrue(fitsRunTable(4, 8));
    }

    public void testFitsRunTableFalseWhenRunsExceedHalfMaxDoc() {
        // 5 runs over 8 docs: 5*2=10 > 8, so the run table does not fit.
        assertFalse(fitsRunTable(5, 8));
    }

    public void testFitsRunTableWithZeroRunsAndDocs() {
        // 0 runs over 0 docs: 0*2=0 which is not > 0, so the run table fits (degenerate empty case).
        assertTrue(fitsRunTable(0, 0));
    }

    public void testFitsRunTableWithSingleDoc() {
        // 1 run over 1 doc: 1*2=2 > 1, so the run table does not fit for a single-doc segment.
        assertFalse(fitsRunTable(1, 1));
    }

    public void testFitsRunTableBoundaryWithTwoDocs() {
        // 1 run over 2 docs: 1*2=2 which is not > 2, so it fits exactly at the two-doc boundary.
        assertTrue(fitsRunTable(1, 2));
    }

    public void testSortedLayoutRoundTrip() throws IOException {
        final int[] perDocOrds = { 0, 0, 1, 1, 2, 2 };
        final int valueCount = 3;
        try (Directory dir = new ByteBuffersDirectory()) {
            final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(valueCount);
            for (final int ord : perDocOrds) {
                writer.add(ord);
            }
            try (
                IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
            ) {
                SortedRunTableLayout.encode(writer, data, meta);
            }
            try (
                IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
                IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT)
            ) {
                final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);
                assertEquals(3, parsed.numRuns());
                final NumericDocValues dv = SortedRunTableLayout.open(parsed, data, perDocOrds.length);
                for (int doc = 0; doc < perDocOrds.length; doc++) {
                    assertTrue(dv.advanceExact(doc));
                    assertEquals("doc " + doc, perDocOrds[doc], dv.longValue());
                }
            }
        }
    }

    public void testSortedSetLayoutRoundTrip() throws IOException {
        final int[][] perDocSets = { { 0, 1 }, { 0, 1 }, { 2, 3 }, { 2, 3 } };
        final int valueCount = 4;
        try (Directory dir = new ByteBuffersDirectory()) {
            final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(valueCount);
            for (final int[] set : perDocSets) {
                writer.add(set);
            }
            try (
                IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
                IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
            ) {
                SortedSetRunTableLayout.encode(writer, data, meta);
            }
            try (
                IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
                IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT)
            ) {
                final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);
                assertEquals(2, parsed.numRuns());
                final SortedNumericDocValues dv = SortedSetRunTableLayout.open(parsed, data, perDocSets.length);
                for (int doc = 0; doc < perDocSets.length; doc++) {
                    assertTrue(dv.advanceExact(doc));
                    assertEquals(perDocSets[doc].length, dv.docValueCount());
                    for (final int expectedOrd : perDocSets[doc]) {
                        assertEquals(expectedOrd, dv.nextValue());
                    }
                }
            }
        }
    }

    private static boolean fitsRunTable(int numRuns, int maxDoc) {
        return (long) numRuns * 2 <= maxDoc;
    }
}
