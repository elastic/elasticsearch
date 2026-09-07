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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link SortedRunMerger}, which k-way merges per-segment series streams by global {@code _tsid}
 * ordinal and drives {@link RunTableSortedOrdinalWriter#addRun}, coalescing adjacent series that share a field
 * ordinal. Series are supplied through in-test {@link SortedSeriesCursor} doubles so the merge order and
 * coalescing can be exercised without a Lucene merge context. The randomized case cross-checks the merged output
 * against a reference built by expanding the same series to a per-doc ordinal stream in merged order.
 */
public class SortedRunMergerTests extends ESTestCase {

    public void testMergeTwoSegmentsInterleaved() throws IOException {
        // Segment A: tsid 0 -> ord 5 (3 docs), tsid 2 -> ord 7 (2 docs).
        // Segment B: tsid 1 -> ord 5 (4 docs), tsid 3 -> ord 5 (1 doc).
        // Merged tsid order 0,1,2,3 -> ords 5,5,7,5 -> runs [5 x7][7 x2][5 x1].
        final SortedSeriesCursor a = cursor(new long[] { 0, 2 }, new int[] { 3, 2 }, new long[] { 5, 7 });
        final SortedSeriesCursor b = cursor(new long[] { 1, 3 }, new int[] { 4, 1 }, new long[] { 5, 5 });
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(8);
        SortedRunMerger.merge(List.of(a, b), writer);

        assertEquals(3, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, 10);
            assertEquals(3, runs.count());
            assertRun(runs, 0, 0, 7, 5);
            assertRun(runs, 1, 7, 2, 7);
            assertRun(runs, 2, 9, 1, 5);
        }
    }

    public void testMergeSameTsidSplitAcrossSegmentsCoalesces() throws IOException {
        final SortedSeriesCursor a = cursor(new long[] { 0 }, new int[] { 3 }, new long[] { 5 });
        final SortedSeriesCursor b = cursor(new long[] { 0 }, new int[] { 2 }, new long[] { 5 });
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(8);
        SortedRunMerger.merge(List.of(a, b), writer);

        assertEquals(1, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, 5);
            assertEquals(1, runs.count());
            assertRun(runs, 0, 0, 5, 5);
        }
    }

    public void testMergeSingleSegment() throws IOException {
        final SortedSeriesCursor a = cursor(new long[] { 0, 1, 2 }, new int[] { 2, 3, 1 }, new long[] { 1, 1, 2 });
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(3);
        SortedRunMerger.merge(List.of(a), writer);

        // Adjacent tsids 0 and 1 share ord 1, so they coalesce.
        assertEquals(2, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, 6);
            assertRun(runs, 0, 0, 5, 1);
            assertRun(runs, 1, 5, 1, 2);
        }
    }

    public void testMergeMatchesReferenceExpansion() throws IOException {
        final int valueCount = randomIntBetween(1, 30);
        final int numSegments = randomIntBetween(1, 6);
        final int numSeries = randomIntBetween(1, 200);
        // A field ordinal per global tsid, shared across every segment that carries the series.
        final long[] fieldOrdByTsid = new long[numSeries];
        for (int t = 0; t < numSeries; t++) {
            fieldOrdByTsid[t] = randomIntBetween(0, valueCount - 1);
        }
        // Per segment, an ascending subset of tsids with random doc counts.
        final int[] docCountByTsid = new int[numSeries];
        final List<SortedSeriesCursor> cursors = new ArrayList<>();
        for (int s = 0; s < numSegments; s++) {
            final List<Long> tsids = new ArrayList<>();
            final List<Integer> docs = new ArrayList<>();
            final List<Long> ords = new ArrayList<>();
            for (int t = 0; t < numSeries; t++) {
                if (randomBoolean()) {
                    final int docCount = randomIntBetween(1, 8);
                    tsids.add((long) t);
                    docs.add(docCount);
                    ords.add(fieldOrdByTsid[t]);
                    docCountByTsid[t] += docCount;
                }
            }
            cursors.add(cursor(toLongArray(tsids), toIntArray(docs), toLongArray(ords)));
        }

        final RunTableSortedOrdinalWriter merged = new RunTableSortedOrdinalWriter(valueCount);
        SortedRunMerger.merge(cursors, merged);

        // Reference: expand to a per-doc ordinal stream in merged (ascending tsid) order and feed the per-doc path.
        final RunTableSortedOrdinalWriter reference = new RunTableSortedOrdinalWriter(valueCount);
        int totalDocs = 0;
        for (int t = 0; t < numSeries; t++) {
            for (int i = 0; i < docCountByTsid[t]; i++) {
                reference.add((int) fieldOrdByTsid[t]);
                totalDocs++;
            }
        }
        if (totalDocs == 0) {
            return; // no segment carried any series; nothing to compare
        }

        try (Directory mergedDir = new ByteBuffersDirectory(); Directory refDir = new ByteBuffersDirectory()) {
            encode(mergedDir, merged);
            encode(refDir, reference);
            final NumericDocValues mergedDv = open(mergedDir, totalDocs);
            final NumericDocValues refDv = open(refDir, totalDocs);
            for (int d = 0; d < totalDocs; d++) {
                assertTrue("doc " + d, mergedDv.advanceExact(d));
                assertTrue("doc " + d, refDv.advanceExact(d));
                assertEquals("doc " + d, refDv.longValue(), mergedDv.longValue());
            }
        }
    }

    private static SortedSeriesCursor cursor(long[] tsidOrds, int[] docCounts, long[] fieldOrds) {
        return new SortedSeriesCursor() {
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
            public int docCount() {
                return docCounts[i];
            }

            @Override
            public long fieldOrd() {
                return fieldOrds[i];
            }
        };
    }

    private static long[] toLongArray(List<Long> list) {
        final long[] out = new long[list.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = list.get(i);
        }
        return out;
    }

    private static int[] toIntArray(List<Integer> list) {
        final int[] out = new int[list.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = list.get(i);
        }
        return out;
    }

    private static void encode(Directory dir, RunTableSortedOrdinalWriter writer) throws IOException {
        try (
            IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
            IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
        ) {
            SortedRunTableLayout.encode(writer, data, meta);
        }
    }

    private static RunTableSortedOrdinalReader.Runs openRuns(Directory dir, int maxDoc) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);
        return SortedRunTableLayout.openRuns(parsed, data, maxDoc);
    }

    private static NumericDocValues open(Directory dir, int maxDoc) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);
        return SortedRunTableLayout.open(parsed, data, maxDoc);
    }

    private static void assertRun(RunTableSortedOrdinalReader.Runs runs, int run, int startDoc, int length, long ordinal) {
        assertEquals("run " + run + " startDoc", startDoc, runs.startDoc(run));
        assertEquals("run " + run + " length", length, runs.length(run));
        assertEquals("run " + run + " ordinal", ordinal, runs.ordinal(run));
    }
}
