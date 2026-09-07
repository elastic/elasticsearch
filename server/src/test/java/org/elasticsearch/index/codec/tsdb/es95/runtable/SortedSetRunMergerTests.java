/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link SortedSetRunMerger}, which k-way merges per-segment series streams by global {@code _tsid}
 * ordinal and drives {@link RunTableSortedSetOrdinalWriter#addRun}, coalescing adjacent series that share an
 * ordinal set. Series are supplied through in-test {@link SortedSetSeriesCursor} doubles so the merge order and
 * coalescing can be exercised without a Lucene merge context. The randomized case cross-checks the merged output
 * against a reference built by expanding the same series to a per-doc set stream in merged order.
 */
public class SortedSetRunMergerTests extends ESTestCase {

    public void testMergeTwoSegmentsInterleaved() throws IOException {
        final SortedSetSeriesCursor a = cursor(new long[] { 0, 2 }, new int[] { 3, 2 }, new int[][] { { 2, 3 }, { 0, 1 } });
        final SortedSetSeriesCursor b = cursor(new long[] { 1, 3 }, new int[] { 4, 1 }, new int[][] { { 2, 3 }, { 2, 3 } });
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        SortedSetRunMerger.merge(List.of(a, b), writer);

        assertEquals(3, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, 10);
            assertRunSet(runs, 0, 0, 7, new int[] { 2, 3 });
            assertRunSet(runs, 1, 7, 2, new int[] { 0, 1 });
            assertRunSet(runs, 2, 9, 1, new int[] { 2, 3 });
        }
    }

    public void testMergeSameTsidSplitAcrossSegmentsCoalesces() throws IOException {
        final SortedSetSeriesCursor a = cursor(new long[] { 0 }, new int[] { 3 }, new int[][] { { 1, 2 } });
        final SortedSetSeriesCursor b = cursor(new long[] { 0 }, new int[] { 2 }, new int[][] { { 1, 2 } });
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        SortedSetRunMerger.merge(List.of(a, b), writer);

        assertEquals(1, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, 5);
            assertRunSet(runs, 0, 0, 5, new int[] { 1, 2 });
        }
    }

    public void testMergeSingleSegment() throws IOException {
        final SortedSetSeriesCursor a = cursor(new long[] { 0, 1, 2 }, new int[] { 2, 3, 1 }, new int[][] { { 1 }, { 1 }, { 2 } });
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(3);
        SortedSetRunMerger.merge(List.of(a), writer);

        assertEquals(2, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, 6);
            assertRunSet(runs, 0, 0, 5, new int[] { 1 });
            assertRunSet(runs, 1, 5, 1, new int[] { 2 });
        }
    }

    public void testMergeMatchesReferenceExpansion() throws IOException {
        final int valueCount = randomIntBetween(1, 25);
        final int numSegments = randomIntBetween(1, 6);
        final int numSeries = randomIntBetween(1, 150);
        final int[][] setByTsid = new int[numSeries][];
        for (int t = 0; t < numSeries; t++) {
            setByTsid[t] = randomSet(valueCount);
        }
        final int[] docCountByTsid = new int[numSeries];
        final List<SortedSetSeriesCursor> cursors = new ArrayList<>();
        for (int s = 0; s < numSegments; s++) {
            final List<Long> tsids = new ArrayList<>();
            final List<Integer> docs = new ArrayList<>();
            final List<int[]> sets = new ArrayList<>();
            for (int t = 0; t < numSeries; t++) {
                if (randomBoolean()) {
                    final int docCount = randomIntBetween(1, 6);
                    tsids.add((long) t);
                    docs.add(docCount);
                    sets.add(setByTsid[t]);
                    docCountByTsid[t] += docCount;
                }
            }
            cursors.add(cursor(toLongArray(tsids), toIntArray(docs), sets.toArray(new int[0][])));
        }

        final RunTableSortedSetOrdinalWriter merged = new RunTableSortedSetOrdinalWriter(valueCount);
        SortedSetRunMerger.merge(cursors, merged);

        final RunTableSortedSetOrdinalWriter reference = new RunTableSortedSetOrdinalWriter(valueCount);
        int totalDocs = 0;
        final int[][] perDocSets = new int[sumOf(docCountByTsid)][];
        for (int t = 0; t < numSeries; t++) {
            for (int i = 0; i < docCountByTsid[t]; i++) {
                reference.add(setByTsid[t]);
                perDocSets[totalDocs++] = setByTsid[t];
            }
        }
        if (totalDocs == 0) {
            return;
        }

        try (Directory mergedDir = new ByteBuffersDirectory(); Directory refDir = new ByteBuffersDirectory()) {
            encode(mergedDir, merged);
            encode(refDir, reference);
            final SortedNumericDocValues mergedDv = open(mergedDir, totalDocs);
            for (int d = 0; d < totalDocs; d++) {
                final boolean present = perDocSets[d].length > 0;
                assertEquals("doc " + d, present, mergedDv.advanceExact(d));
                if (present) {
                    assertDocSet(mergedDv, perDocSets[d]);
                }
            }
        }
    }

    private static SortedSetSeriesCursor cursor(long[] tsidOrds, int[] docCounts, int[][] sets) {
        return new SortedSetSeriesCursor() {
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
            public int ordCount() {
                return sets[i].length;
            }

            @Override
            public int ordAt(int index) {
                return sets[i][index];
            }
        };
    }

    private static int[] randomSet(int valueCount) {
        final int size = randomIntBetween(0, valueCount);
        final List<Integer> pool = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            pool.add(i);
        }
        Collections.shuffle(pool, random());
        final int[] set = new int[size];
        for (int i = 0; i < size; i++) {
            set[i] = pool.get(i);
        }
        Arrays.sort(set);
        return set;
    }

    private static int sumOf(int[] values) {
        int sum = 0;
        for (final int value : values) {
            sum += value;
        }
        return sum;
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

    private static void encode(Directory dir, RunTableSortedSetOrdinalWriter writer) throws IOException {
        try (
            IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
            IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
        ) {
            SortedSetRunTableLayout.encode(writer, data, meta);
        }
    }

    private static RunTableSortedSetOrdinalReader.Runs openRuns(Directory dir, int maxDoc) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);
        return SortedSetRunTableLayout.openRuns(parsed, data, maxDoc);
    }

    private static SortedNumericDocValues open(Directory dir, int maxDoc) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);
        return SortedSetRunTableLayout.open(parsed, data, maxDoc);
    }

    private static void assertRunSet(RunTableSortedSetOrdinalReader.Runs runs, int run, int startDoc, int length, int[] expectedOrds) {
        assertEquals("run " + run + " startDoc", startDoc, runs.startDoc(run));
        assertEquals("run " + run + " length", length, runs.length(run));
        assertEquals("run " + run + " ordCount", expectedOrds.length, runs.ordCount(run));
        for (int i = 0; i < expectedOrds.length; i++) {
            assertEquals("run " + run + " ord " + i, expectedOrds[i], runs.ordAt(run, i));
        }
    }

    private static void assertDocSet(SortedNumericDocValues dv, int[] expectedSet) throws IOException {
        assertEquals(expectedSet.length, dv.docValueCount());
        for (int i = 0; i < expectedSet.length; i++) {
            assertEquals(expectedSet[i], dv.nextValue());
        }
    }
}
