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
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedOrdinalWriter.Stats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Unit tests for the single-valued {@link RunTableSortedOrdinalWriter}/{@link RunTableSortedOrdinalReader}
 * pair in isolation, driving the writer and reader directly rather than through the ES95 format. Covers the
 * dense and sparse (contiguous absence via the {@code K} sentinel) round-trips across the sequential,
 * random-forward, and {@code nextDoc} access patterns, the shared data/meta stream seam, and the exact
 * encoded size, each exact byte count paired with a relationship assertion so a wrong golden cannot pass.
 */
public class RunTableSortedOrdinalTests extends ESTestCase {

    public void testWorkedExample() throws IOException {
        final int[] perDocOrds = { 1, 1, 1, 1, 2, 2, 2, 2, 0, 0, 0, 0, 1, 1, 1, 1 };
        final int valueCount = 3;
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            assertEquals(4, stats.numRuns());
            verifyAllDriveModes(dir, perDocOrds, 0);
        }
    }

    public void testRunViewWorkedExample() throws IOException {
        final int[] perDocOrds = { 1, 1, 1, 1, 2, 2, 2, 2, 0, 0, 0, 0, 1, 1, 1, 1 };
        final int valueCount = 3;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, 0, 0);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, perDocOrds.length, 0);
            assertEquals(4, runs.count());
            assertEquals(valueCount, runs.sentinel());
            assertRun(runs, 0, 0, 4, 1);
            assertRun(runs, 1, 4, 4, 2);
            assertRun(runs, 2, 8, 4, 0);
            assertRun(runs, 3, 12, 4, 1);
        }
    }

    public void testRunViewExposesSentinelRuns() throws IOException {
        final int valueCount = 3; // sentinel ordinal == 3
        final int[] perDocOrds = { 1, 1, 3, 3, 3, 2, 2, 0, 0, 3, 1, 1 };
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, 0, 0);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, perDocOrds.length, 0);
            // Runs: [1,1][3,3,3][2,2][0,0][3][1,1], two of them sentinel.
            assertEquals(6, runs.count());
            assertEquals(valueCount, runs.sentinel());
            assertRun(runs, 0, 0, 2, 1);
            assertRun(runs, 1, 2, 3, valueCount);
            assertRun(runs, 2, 5, 2, 2);
            assertRun(runs, 3, 7, 2, 0);
            assertRun(runs, 4, 9, 1, valueCount);
            assertRun(runs, 5, 10, 2, 1);
        }
    }

    public void testRunViewMatchesPerDocReader() throws IOException {
        final int valueCount = randomIntBetween(1, 50);
        final int numSeries = randomIntBetween(1, 400);
        final int[] perDocOrds = randomPiecewiseConstant(numSeries, valueCount);
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, 0, 0);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, perDocOrds.length, 0);
            final NumericDocValues perDoc = open(dir, perDocOrds.length, 0);
            int runStart = 0;
            for (int run = 0; run < runs.count(); run++) {
                assertEquals("run " + run + " startDoc", runStart, runs.startDoc(run));
                final int length = runs.length(run);
                assertTrue("run " + run + " length must be positive", length > 0);
                final long ord = runs.ordinal(run);
                for (int i = 0; i < length; i++) {
                    final int doc = runStart + i;
                    assertEquals("doc " + doc, perDocOrds[doc], ord);
                    assertTrue("doc " + doc, perDoc.advanceExact(doc));
                    assertEquals("doc " + doc, perDocOrds[doc], (int) perDoc.longValue());
                }
                runStart += length;
            }
            assertEquals("runs must cover every doc exactly once", perDocOrds.length, runStart);
        }
    }

    public void testAddRunProducesExpectedRuns() throws IOException {
        final int valueCount = 3;
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(valueCount);
        writer.addRun(1, 4);
        writer.addRun(2, 4);
        writer.addRun(0, 4);
        writer.addRun(1, 4);
        assertEquals(4, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedOrdinalReader.Runs runs = openRuns(dir, 16, 0);
            assertEquals(4, runs.count());
            assertRun(runs, 0, 0, 4, 1);
            assertRun(runs, 1, 4, 4, 2);
            assertRun(runs, 2, 8, 4, 0);
            assertRun(runs, 3, 12, 4, 1);
        }
    }

    public void testAddRunCoalescesEqualAdjacentRuns() {
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(3);
        writer.addRun(1, 3);
        writer.addRun(1, 2);
        writer.addRun(2, 1);
        assertEquals(2, writer.numRuns());
    }

    public void testAddRunMatchesPerDocAddForRandomRuns() throws IOException {
        final int valueCount = randomIntBetween(1, 40);
        final int numRuns = randomIntBetween(1, 300);
        final int[] runOrds = new int[numRuns];
        final int[] runLengths = new int[numRuns];
        for (int r = 0; r < numRuns; r++) {
            runOrds[r] = randomIntBetween(0, valueCount - 1);
            runLengths[r] = randomIntBetween(1, 12);
        }
        int total = 0;
        for (final int length : runLengths) {
            total += length;
        }
        final int[] perDocOrds = new int[total];
        int doc = 0;
        for (int r = 0; r < numRuns; r++) {
            for (int i = 0; i < runLengths[r]; i++) {
                perDocOrds[doc++] = runOrds[r];
            }
        }
        final RunTableSortedOrdinalWriter runWriter = new RunTableSortedOrdinalWriter(valueCount);
        for (int r = 0; r < numRuns; r++) {
            runWriter.addRun(runOrds[r], runLengths[r]);
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, runWriter);
            final NumericDocValues perDoc = open(dir, total, 0);
            for (int d = 0; d < total; d++) {
                assertTrue("doc " + d, perDoc.advanceExact(d));
                assertEquals("doc " + d, perDocOrds[d], (int) perDoc.longValue());
            }
        }
    }

    public void testSingleRun() throws IOException {
        final int maxDoc = 500;
        final int[] perDocOrds = new int[maxDoc];
        Arrays.fill(perDocOrds, 7);
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, 8, 0, 0);
            assertEquals(1, stats.numRuns());
            verifyAllDriveModes(dir, perDocOrds, 0);
        }
    }

    public void testExceedsThresholdReturnsTrueWhenRunsExceedHalf() {
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(4);
        writer.add(0);
        writer.add(1);
        writer.add(2);
        // 3 runs, maxDoc=4: 3*2=6 > 4, so threshold is exceeded.
        assertTrue(writer.exceedsThreshold(4));

        final RunTableSortedOrdinalWriter writer2 = new RunTableSortedOrdinalWriter(4);
        writer2.add(0);
        writer2.add(1);
        // 2 runs, maxDoc=4: 2*2=4 which is not > 4, so threshold is not exceeded.
        assertFalse(writer2.exceedsThreshold(4));
    }

    public void testRandomPiecewiseConstantRoundTrip() throws IOException {
        final int valueCount = randomIntBetween(1, 50);
        final int numSeries = randomIntBetween(1, 400);
        final int[] perDocOrds = randomPiecewiseConstant(numSeries, valueCount);
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            // NOTE: adjacent series sharing an ord merge into one run, so runs never exceed series.
            assertTrue("numRuns " + stats.numRuns() + " > numSeries " + numSeries, stats.numRuns() <= numSeries);
            verifyAllDriveModes(dir, perDocOrds, 0);
        }
    }

    public void testSharedStreamOffsets() throws IOException {
        // NOTE: writing a prefix into both streams forces a non-zero data start, exercising the seam
        // a later task relies on when handing the writer the codec's shared data/meta outputs.
        final int[] perDocOrds = { 3, 3, 1, 1, 1, 0, 2, 2 };
        final int valueCount = 4;
        final int dataPrefix = 37;
        final int metaPrefix = 11;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, dataPrefix, metaPrefix);
            verifyAllDriveModes(dir, perDocOrds, metaPrefix);
        }
    }

    public void testExactEncodedSize() throws IOException {
        // NOTE: three long runs over 30000 docs. The encoding cost tracks the run count, not the doc
        // count, so it must land far below the naive per-doc bit-packed lower bound.
        final int perRun = 10000;
        final int valueCount = 3;
        final int[] perDocOrds = new int[3 * perRun];
        Arrays.fill(perDocOrds, 0, perRun, 1);
        Arrays.fill(perDocOrds, perRun, 2 * perRun, 2);
        Arrays.fill(perDocOrds, 2 * perRun, 3 * perRun, 0);
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            assertEquals(3, stats.numRuns());
            assertEquals(28L, stats.totalBytes());

            final int bitsPerOrd = PackedInts.bitsRequired(valueCount - 1);
            final long naiveLowerBoundBytes = (long) perDocOrds.length * bitsPerOrd / 8;
            assertTrue(
                "totalBytes " + stats.totalBytes() + " not far below naive " + naiveLowerBoundBytes,
                stats.totalBytes() * 10 < naiveLowerBoundBytes
            );
        }
    }

    public void testSparseRoundTrip() throws IOException {
        // Absent docs (sentinel == valueCount) form contiguous spans between value-bearing runs.
        final int valueCount = 3;
        final int[] perDocOrds = { 1, 1, 3, 3, 3, 2, 2, 0, 0, 3, 1, 1 };
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            // Runs: [1,1][3,3,3][2,2][0,0][3][1,1] == 6 runs, two of them sentinel.
            assertEquals(6, stats.numRuns());
            verifySparse(dir, perDocOrds, valueCount, 0);
        }
    }

    public void testSparseLeadingAndTrailingAbsent() throws IOException {
        // The first and last docs are absent, so iteration must open past a leading sentinel run and
        // terminate before a trailing one.
        final int valueCount = 4;
        final int[] perDocOrds = { 4, 4, 1, 1, 1, 2, 4, 4, 4 };
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, 0, 0);
            verifySparse(dir, perDocOrds, valueCount, 0);
        }
    }

    public void testSparseExactEncodedSize() throws IOException {
        // Three runs over 30000 docs: a value run, a sentinel gap, then a value run. The sentinel widens
        // the ordinal width to hold ord 2 (== valueCount), but the cost still tracks the run count.
        final int perRun = 10000;
        final int valueCount = 2;
        final int[] perDocOrds = new int[3 * perRun];
        Arrays.fill(perDocOrds, 0, perRun, 1);
        Arrays.fill(perDocOrds, perRun, 2 * perRun, valueCount); // absent sentinel
        Arrays.fill(perDocOrds, 2 * perRun, 3 * perRun, 0);
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            assertEquals(3, stats.numRuns());
            assertEquals(28L, stats.totalBytes());

            // The sentinel widens the ordinal width, but the cost still tracks runs, not docs: it must land
            // far below the naive per-doc bit-packed lower bound so the exact count above cannot silently drift.
            final int bitsPerOrd = PackedInts.bitsRequired(valueCount);
            final long naiveLowerBoundBytes = (long) perDocOrds.length * bitsPerOrd / 8;
            assertTrue(
                "totalBytes " + stats.totalBytes() + " not far below naive " + naiveLowerBoundBytes,
                stats.totalBytes() * 10 < naiveLowerBoundBytes
            );
            verifySparse(dir, perDocOrds, valueCount, 0);
        }
    }

    public void testFullyAbsentAllSentinel() throws IOException {
        final int valueCount = 3;
        final int numDocs = randomIntBetween(1, 500);
        final int[] perDocOrds = new int[numDocs];
        Arrays.fill(perDocOrds, valueCount); // all docs absent (sentinel == valueCount)
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            assertEquals(1, stats.numRuns());
            verifySparse(dir, perDocOrds, valueCount, 0);
        }
    }

    public void testFullyDenseStillAlwaysPresent() throws IOException {
        // No sentinel in the stream: advanceExact is always true and nextDoc visits every doc.
        final int[] perDocOrds = { 0, 0, 1, 1, 2, 2, 2, 0 };
        final int valueCount = 3;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocOrds, valueCount, 0, 0);
            verifySparse(dir, perDocOrds, valueCount, 0);
            verifyAllDriveModes(dir, perDocOrds, 0);
        }
    }

    public void testOrdinalColumnReadOffHeapOnDemand() throws IOException {
        // Every doc opens a new run and ordinals span a 10-bit range, so the ordinal column is several KB.
        // If the reader slurped it into an on-heap PackedInts.Mutable at open time, the byte counter below
        // would climb to the whole column size at open; the DirectReader path leaves it untouched.
        final int numRuns = 4096;
        final int valueCount = 1024;
        final int[] perDocOrds = new int[numRuns];
        for (int i = 0; i < numRuns; i++) {
            perDocOrds[i] = i % valueCount;
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocOrds, valueCount, 0, 0);
            assertEquals(numRuns, stats.numRuns());

            final int bitsPerOrd = DirectWriter.bitsRequired(valueCount - 1);
            final long ordColumnBytes = DirectWriter.bytesRequired(numRuns, bitsPerOrd);
            assertTrue("ordinal column should be several KB, was " + ordColumnBytes, ordColumnBytes > 4096);

            final long[] counter = new long[1];
            try (
                IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
                IndexInput rawData = dir.openInput("data.bin", IOContext.DEFAULT)
            ) {
                final CountingIndexInput data = new CountingIndexInput(rawData, counter);
                final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);

                counter[0] = 0;
                final NumericDocValues dv = SortedRunTableLayout.open(parsed, data, numRuns);
                final long bytesAtOpen = counter[0];
                assertEquals("open must not read the ordinal column, it is served off-heap on demand", 0L, bytesAtOpen);
                assertTrue(
                    "bytes read at open (" + bytesAtOpen + ") must be below the column size " + ordColumnBytes,
                    bytesAtOpen < ordColumnBytes
                );

                // A single random seek reads only around the target run, far less than the whole column.
                final int target = numRuns / 2 + 7;
                assertTrue(dv.advanceExact(target));
                assertEquals(perDocOrds[target], dv.longValue());
                final long bytesAfterSeek = counter[0];
                assertTrue("a single get must read on demand", bytesAfterSeek > bytesAtOpen);
                assertTrue(
                    "a single get (" + bytesAfterSeek + ") must read far less than the whole column " + ordColumnBytes,
                    bytesAfterSeek < ordColumnBytes
                );

                // A full sequential scan pulls every ordinal, so the counter climbs well past the single-seek cost.
                for (int doc = 0; doc < numRuns; doc++) {
                    assertTrue(dv.advanceExact(doc));
                    assertEquals("doc " + doc, perDocOrds[doc], dv.longValue());
                }
                assertTrue("a full scan must read more than a single seek", counter[0] > bytesAfterSeek);
            }
        }
    }

    private void verifySparse(Directory dir, int[] perDocOrds, int sentinel, int metaPrefix) throws IOException {
        // advanceExact: false exactly for absent docs, true with the right ord otherwise.
        final NumericDocValues exact = open(dir, perDocOrds.length, metaPrefix);
        for (int doc = 0; doc < perDocOrds.length; doc++) {
            final boolean present = perDocOrds[doc] != sentinel;
            assertEquals("doc " + doc, present, exact.advanceExact(doc));
            if (present) {
                assertEquals("doc " + doc, perDocOrds[doc], exact.longValue());
            }
        }
        // nextDoc: visits exactly the present docs in order, then NO_MORE_DOCS.
        final NumericDocValues iter = open(dir, perDocOrds.length, metaPrefix);
        int doc = -1;
        for (int expectedDoc = 0; expectedDoc < perDocOrds.length; expectedDoc++) {
            if (perDocOrds[expectedDoc] == sentinel) {
                continue;
            }
            doc = iter.nextDoc();
            assertEquals(expectedDoc, doc);
            assertEquals("doc " + doc, perDocOrds[expectedDoc], iter.longValue());
        }
        assertEquals(NO_MORE_DOCS, iter.nextDoc());
    }

    private static int[] randomPiecewiseConstant(int numSeries, int valueCount) {
        final int[][] spans = new int[numSeries][];
        int total = 0;
        for (int s = 0; s < numSeries; s++) {
            final int len = randomIntBetween(1, 20);
            spans[s] = new int[] { randomIntBetween(0, valueCount - 1), len };
            total += len;
        }
        final int[] perDocOrds = new int[total];
        int doc = 0;
        for (int s = 0; s < numSeries; s++) {
            for (int i = 0; i < spans[s][1]; i++) {
                perDocOrds[doc++] = spans[s][0];
            }
        }
        return perDocOrds;
    }

    private static Stats write(Directory dir, int[] perDocOrds, int valueCount, int dataPrefix, int metaPrefix) throws IOException {
        final RunTableSortedOrdinalWriter writer = new RunTableSortedOrdinalWriter(valueCount);
        for (final int ord : perDocOrds) {
            writer.add(ord);
        }
        try (
            IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
            IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
        ) {
            for (int i = 0; i < dataPrefix; i++) {
                data.writeByte((byte) i);
            }
            for (int i = 0; i < metaPrefix; i++) {
                meta.writeByte((byte) i);
            }
            return SortedRunTableLayout.encode(writer, data, meta);
        }
    }

    private static Stats encode(Directory dir, RunTableSortedOrdinalWriter writer) throws IOException {
        try (
            IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
            IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
        ) {
            return SortedRunTableLayout.encode(writer, data, meta);
        }
    }

    private void verifyAllDriveModes(Directory dir, int[] expected, int metaPrefix) throws IOException {
        verifySequential(dir, expected, metaPrefix);
        verifyRandomIncreasing(dir, expected, metaPrefix);
        verifyNextDoc(dir, expected, metaPrefix);
    }

    private NumericDocValues open(Directory dir, int maxDoc, int metaPrefix) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        meta.seek(metaPrefix);
        final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);
        return SortedRunTableLayout.open(parsed, data, maxDoc);
    }

    private RunTableSortedOrdinalReader.Runs openRuns(Directory dir, int maxDoc, int metaPrefix) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        meta.seek(metaPrefix);
        final RunTableSortedOrdinalReader.Meta parsed = SortedRunTableLayout.readMeta(meta);
        return SortedRunTableLayout.openRuns(parsed, data, maxDoc);
    }

    private void assertRun(RunTableSortedOrdinalReader.Runs runs, int run, int startDoc, int length, long ordinal) {
        assertEquals("run " + run + " startDoc", startDoc, runs.startDoc(run));
        assertEquals("run " + run + " length", length, runs.length(run));
        assertEquals("run " + run + " ordinal", ordinal, runs.ordinal(run));
    }

    private void verifySequential(Directory dir, int[] expected, int metaPrefix) throws IOException {
        final NumericDocValues dv = open(dir, expected.length, metaPrefix);
        for (int doc = 0; doc < expected.length; doc++) {
            assertTrue(dv.advanceExact(doc));
            assertEquals("doc " + doc, expected[doc], dv.longValue());
        }
    }

    private void verifyRandomIncreasing(Directory dir, int[] expected, int metaPrefix) throws IOException {
        final NumericDocValues dv = open(dir, expected.length, metaPrefix);
        int doc = -1;
        while (true) {
            final int next = doc + 1 + randomIntBetween(0, 30);
            if (next >= expected.length) {
                break;
            }
            doc = next;
            assertTrue(dv.advanceExact(doc));
            assertEquals("doc " + doc, expected[doc], dv.longValue());
        }
    }

    private void verifyNextDoc(Directory dir, int[] expected, int metaPrefix) throws IOException {
        final NumericDocValues dv = open(dir, expected.length, metaPrefix);
        int expectedDoc = 0;
        int doc;
        while ((doc = dv.nextDoc()) != NO_MORE_DOCS) {
            assertEquals(expectedDoc, doc);
            assertEquals("doc " + doc, expected[doc], dv.longValue());
            expectedDoc++;
        }
        assertEquals(expected.length, expectedDoc);
    }

}
