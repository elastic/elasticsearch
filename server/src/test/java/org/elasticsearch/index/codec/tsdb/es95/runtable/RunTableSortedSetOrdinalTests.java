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
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetOrdinalWriter.Stats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Unit tests for the multi-valued {@link RunTableSortedSetOrdinalWriter}/{@link RunTableSortedSetOrdinalReader}
 * pair in isolation, driving the writer and reader directly rather than through the ES95 format. Covers the
 * dense and sparse (empty-set absence) round-trips across the sequential, random-forward, and {@code nextDoc}
 * access patterns, the ascending per-doc ordinal contract, the shared data/meta stream seam, and the exact
 * encoded size paired with a relationship assertion so a wrong golden cannot pass.
 */
public class RunTableSortedSetOrdinalTests extends ESTestCase {

    public void testWorkedExample() throws IOException {
        final int[][] perDocSets = {
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 } };
        final int valueCount = 4;
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertEquals(3, stats.numRuns());
            verifyAllDriveModes(dir, perDocSets, 0);
        }
    }

    public void testRunViewWorkedExample() throws IOException {
        final int[][] perDocSets = {
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 0, 1, 2 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 },
            { 2, 3 } };
        final int valueCount = 4;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocSets, valueCount, 0, 0);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, perDocSets.length, 0);
            assertEquals(3, runs.count());
            assertRunSet(runs, 0, 0, 4, new int[] { 2, 3 });
            assertRunSet(runs, 1, 4, 4, new int[] { 0, 1, 2 });
            assertRunSet(runs, 2, 8, 4, new int[] { 2, 3 });
        }
    }

    public void testRunViewExposesEmptySetRuns() throws IOException {
        final int[][] perDocSets = { {}, { 0, 1 }, { 0, 1 }, { 0, 1 }, {} };
        final int valueCount = 2;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocSets, valueCount, 0, 0);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, perDocSets.length, 0);
            assertEquals(3, runs.count());
            assertRunSet(runs, 0, 0, 1, new int[0]);
            assertRunSet(runs, 1, 1, 3, new int[] { 0, 1 });
            assertRunSet(runs, 2, 4, 1, new int[0]);
        }
    }

    public void testRunViewMatchesPerDocReader() throws IOException {
        final int valueCount = randomIntBetween(1, 50);
        final int numSeries = randomIntBetween(1, 300);
        final int[][] perDocSets = randomPiecewiseConstant(numSeries, valueCount);
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocSets, valueCount, 0, 0);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, perDocSets.length, 0);
            final SortedNumericDocValues perDoc = open(dir, perDocSets.length, 0);
            int runStart = 0;
            for (int run = 0; run < runs.count(); run++) {
                assertEquals("run " + run + " startDoc", runStart, runs.startDoc(run));
                final int length = runs.length(run);
                assertTrue("run " + run + " length must be positive", length > 0);
                final int ordCount = runs.ordCount(run);
                for (int i = 0; i < length; i++) {
                    final int doc = runStart + i;
                    assertEquals("doc " + doc + " set size", ordCount, perDocSets[doc].length);
                    final boolean present = ordCount > 0;
                    assertEquals("doc " + doc + " presence", present, perDoc.advanceExact(doc));
                    if (present) {
                        assertEquals("doc " + doc + " count", ordCount, perDoc.docValueCount());
                    }
                    for (int o = 0; o < ordCount; o++) {
                        assertEquals("run " + run + " ord " + o, perDocSets[doc][o], runs.ordAt(run, o));
                    }
                }
                runStart += length;
            }
            assertEquals("runs must cover every doc exactly once", perDocSets.length, runStart);
        }
    }

    public void testAddRunProducesExpectedRuns() throws IOException {
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        writer.addRun(new int[] { 2, 3 }, 2, 4);
        writer.addRun(new int[] { 0, 1, 2 }, 3, 4);
        writer.addRun(new int[] { 2, 3 }, 2, 4);
        assertEquals(3, writer.numRuns());
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, writer);
            final RunTableSortedSetOrdinalReader.Runs runs = openRuns(dir, 12, 0);
            assertEquals(3, runs.count());
            assertRunSet(runs, 0, 0, 4, new int[] { 2, 3 });
            assertRunSet(runs, 1, 4, 4, new int[] { 0, 1, 2 });
            assertRunSet(runs, 2, 8, 4, new int[] { 2, 3 });
        }
    }

    public void testAddRunCoalescesEqualAdjacentSets() {
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(3);
        writer.addRun(new int[] { 0, 1 }, 2, 3);
        writer.addRun(new int[] { 0, 1 }, 2, 2);
        writer.addRun(new int[] { 2 }, 1, 1);
        assertEquals(2, writer.numRuns());
    }

    public void testAddRunMatchesPerDocAddForRandomRuns() throws IOException {
        final int valueCount = randomIntBetween(1, 40);
        final int numRuns = randomIntBetween(1, 250);
        final int[][] runSets = new int[numRuns][];
        final int[] runLengths = new int[numRuns];
        int total = 0;
        for (int r = 0; r < numRuns; r++) {
            runSets[r] = randomSet(valueCount);
            runLengths[r] = randomIntBetween(1, 10);
            total += runLengths[r];
        }
        final int[][] perDocSets = new int[total][];
        int doc = 0;
        for (int r = 0; r < numRuns; r++) {
            for (int i = 0; i < runLengths[r]; i++) {
                perDocSets[doc++] = runSets[r];
            }
        }
        final RunTableSortedSetOrdinalWriter runWriter = new RunTableSortedSetOrdinalWriter(valueCount);
        for (int r = 0; r < numRuns; r++) {
            runWriter.addRun(runSets[r], runSets[r].length, runLengths[r]);
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            encode(dir, runWriter);
            final SortedNumericDocValues perDoc = open(dir, total, 0);
            for (int d = 0; d < total; d++) {
                assertTrue("doc " + d, perDoc.advanceExact(d));
                assertDocSet(perDoc, perDocSets[d]);
            }
        }
    }

    public void testSingleRun() throws IOException {
        final int maxDoc = 300;
        final int[][] perDocSets = new int[maxDoc][];
        for (int i = 0; i < maxDoc; i++) {
            perDocSets[i] = new int[] { 1, 4, 5 };
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, 8, 0, 0);
            assertEquals(1, stats.numRuns());
            verifyAllDriveModes(dir, perDocSets, 0);
        }
    }

    public void testExceedsThresholdReturnsTrueWhenRunsExceedHalf() {
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        writer.add(new int[] { 0 });
        writer.add(new int[] { 1 });
        writer.add(new int[] { 2 });
        // 3 runs, maxDoc=4: 3*2=6 > 4, so threshold is exceeded.
        assertTrue(writer.exceedsThreshold(4));

        final RunTableSortedSetOrdinalWriter writer2 = new RunTableSortedSetOrdinalWriter(4);
        writer2.add(new int[] { 0 });
        writer2.add(new int[] { 1 });
        // 2 runs, maxDoc=4: 2*2=4 which is not > 4, so threshold is not exceeded.
        assertFalse(writer2.exceedsThreshold(4));
    }

    public void testSortedSetOrdinalWriterRetainsOnlyLastSetDuringAccumulation() {
        // Each run carries 4 docs with the same two-ordinal set. Three distinct sets → three runs.
        // totalOrds must equal 3 runs * 2 ords = 6, not 12 (the run table stores one copy per run).
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        final int docsPerRun = 4;
        for (int i = 0; i < docsPerRun; i++) {
            writer.add(new int[] { 0, 1 });
        }
        for (int i = 0; i < docsPerRun; i++) {
            writer.add(new int[] { 1, 2 });
        }
        for (int i = 0; i < docsPerRun; i++) {
            writer.add(new int[] { 2, 3 });
        }
        assertEquals(3, writer.numRuns());
        assertEquals(6, writer.totalOrds());
    }

    public void testRandomRoundTrip() throws IOException {
        final int valueCount = randomIntBetween(1, 50);
        final int numSeries = randomIntBetween(1, 300);
        final int[][] perDocSets = randomPiecewiseConstant(numSeries, valueCount);
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertTrue("numRuns " + stats.numRuns() + " > numSeries " + numSeries, stats.numRuns() <= numSeries);
            verifyAllDriveModes(dir, perDocSets, 0);
        }
    }

    public void testSharedStreamOffsets() throws IOException {
        // NOTE: a non-zero prefix in both streams forces a non-zero data start, exercising the seam a
        // later task relies on when handing the writer the codec's shared data/meta outputs.
        final int[][] perDocSets = { { 0, 2 }, { 0, 2 }, { 1 }, { 1 }, { 0, 1, 3 }, { 0, 1, 3 } };
        final int valueCount = 4;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocSets, valueCount, 29, 13);
            verifyAllDriveModes(dir, perDocSets, 13);
        }
    }

    public void testExactEncodedSize() throws IOException {
        // NOTE: three long runs over 30000 docs. The encoding cost tracks the run count, not the doc
        // count, so it must land far below the naive per-doc bit-packed lower bound.
        final int perRun = 10000;
        final int valueCount = 8;
        final int[][] perDocSets = new int[3 * perRun][];
        fill(perDocSets, 0, perRun, new int[] { 2, 3 });
        fill(perDocSets, perRun, 2 * perRun, new int[] { 0, 1, 5 });
        fill(perDocSets, 2 * perRun, 3 * perRun, new int[] { 4, 6, 7 });
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertEquals(3, stats.numRuns());
            assertEquals(53L, stats.totalBytes());

            final int bitsPerOrd = PackedInts.bitsRequired(valueCount - 1);
            // NOTE: naive lower bound counts one ord per doc, though most docs carry more, so the real
            // per-doc layout is larger still; the run table beating this bound is a conservative floor.
            final long naiveLowerBoundBytes = (long) perDocSets.length * bitsPerOrd / 8;
            assertTrue(
                "totalBytes " + stats.totalBytes() + " not far below naive " + naiveLowerBoundBytes,
                stats.totalBytes() * 10 < naiveLowerBoundBytes
            );
        }
    }

    public void testOrdStreamReadOffHeapOnDemand() throws IOException {
        // Every doc opens a new run carrying a two-ord set drawn from a 10-bit range, so the flattened
        // ordStream column is several KB. If the reader slurped it into an on-heap PackedInts.Mutable at
        // open time, the byte counter below would climb to the whole column size at open; the DirectReader
        // path leaves it untouched.
        final int numRuns = 3000;
        final int valueCount = 1024;
        final int[][] perDocSets = new int[numRuns][];
        for (int i = 0; i < numRuns; i++) {
            perDocSets[i] = new int[] { i % (valueCount - 1), valueCount - 1 };
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertEquals(numRuns, stats.numRuns());

            final int totalOrds = 2 * numRuns;
            final int bitsPerOrd = DirectWriter.bitsRequired(valueCount - 1);
            final long ordColumnBytes = DirectWriter.bytesRequired(totalOrds, bitsPerOrd);
            assertTrue("ordStream column should be several KB, was " + ordColumnBytes, ordColumnBytes > 4096);

            final long[] counter = new long[1];
            try (
                IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
                IndexInput rawData = dir.openInput("data.bin", IOContext.DEFAULT)
            ) {
                final CountingIndexInput data = new CountingIndexInput(rawData, counter);
                final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);

                counter[0] = 0;
                final SortedNumericDocValues dv = SortedSetRunTableLayout.open(parsed, data, numRuns);
                final long bytesAtOpen = counter[0];
                assertEquals("open must not read the ordStream column, it is served off-heap on demand", 0L, bytesAtOpen);
                assertTrue(
                    "bytes read at open (" + bytesAtOpen + ") must be below the column size " + ordColumnBytes,
                    bytesAtOpen < ordColumnBytes
                );

                // A single random seek plus its slice walk reads only around the target run.
                final int target = numRuns / 2 + 7;
                assertTrue(dv.advanceExact(target));
                assertDocSet(dv, perDocSets[target]);
                final long bytesAfterSeek = counter[0];
                assertTrue("a single get must read on demand", bytesAfterSeek > bytesAtOpen);
                assertTrue(
                    "a single get (" + bytesAfterSeek + ") must read far less than the whole column " + ordColumnBytes,
                    bytesAfterSeek < ordColumnBytes
                );

                // A full sequential scan walks every slice, so the counter climbs well past the single-seek cost.
                for (int doc = 0; doc < numRuns; doc++) {
                    assertTrue(dv.advanceExact(doc));
                    assertDocSet(dv, perDocSets[doc]);
                }
                assertTrue("a full scan must read more than a single seek", counter[0] > bytesAfterSeek);
            }
        }
    }

    public void testSortedSetSparseLeadingAndTrailingAbsent() throws IOException {
        final int[][] perDocSets = { {}, { 0, 1 }, { 0, 1 }, { 0, 1 }, {} };
        final int valueCount = 2;
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertEquals(3, stats.numRuns());

            final SortedNumericDocValues dv = open(dir, perDocSets.length, 0);
            assertFalse(dv.advanceExact(0));
            assertTrue(dv.advanceExact(1));
            assertDocSet(dv, new int[] { 0, 1 });
            assertTrue(dv.advanceExact(3));
            assertDocSet(dv, new int[] { 0, 1 });
            assertFalse(dv.advanceExact(4));

            final SortedNumericDocValues dv2 = open(dir, perDocSets.length, 0);
            assertEquals(1, dv2.nextDoc());
            assertDocSet(dv2, new int[] { 0, 1 });
            assertEquals(NO_MORE_DOCS, dv2.advance(4));
        }
    }

    public void testSortedSetSparseOnlyOneDocPresent() throws IOException {
        final int[][] perDocSets = { {}, {}, { 0, 1 }, {}, {} };
        final int valueCount = 2;
        try (Directory dir = new ByteBuffersDirectory()) {
            write(dir, perDocSets, valueCount, 0, 0);

            final SortedNumericDocValues dv = open(dir, perDocSets.length, 0);
            assertEquals(2, dv.nextDoc());
            assertDocSet(dv, new int[] { 0, 1 });
            assertEquals(NO_MORE_DOCS, dv.nextDoc());
        }
    }

    public void testSortedSetFullyAbsent() throws IOException {
        final int valueCount = 3;
        final int numDocs = randomIntBetween(1, 500);
        final int[][] perDocSets = new int[numDocs][0]; // all docs have empty set
        try (Directory dir = new ByteBuffersDirectory()) {
            final Stats stats = write(dir, perDocSets, valueCount, 0, 0);
            assertEquals(1, stats.numRuns());
            final SortedNumericDocValues dv = open(dir, numDocs, 0);
            for (int doc = 0; doc < numDocs; doc++) {
                assertFalse("doc " + doc + " must be absent", dv.advanceExact(doc));
            }
            final SortedNumericDocValues iter = open(dir, numDocs, 0);
            assertEquals(NO_MORE_DOCS, iter.nextDoc());
        }
    }

    public void testSortedSetConsecutiveEmptyRunsNotPossible() {
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(4);
        writer.add(new int[0]);
        writer.add(new int[0]);
        assertEquals(1, writer.numRuns());
        writer.add(new int[] { 0, 1 });
        assertEquals(2, writer.numRuns());
    }

    private static void fill(int[][] sets, int from, int to, int[] value) {
        for (int i = from; i < to; i++) {
            sets[i] = value;
        }
    }

    private static int[][] randomPiecewiseConstant(int numSeries, int valueCount) {
        final int[][] sets = new int[numSeries][];
        final int[] lengths = new int[numSeries];
        int total = 0;
        for (int s = 0; s < numSeries; s++) {
            sets[s] = randomSet(valueCount);
            lengths[s] = randomIntBetween(1, 15);
            total += lengths[s];
        }
        final int[][] perDocSets = new int[total][];
        int doc = 0;
        for (int s = 0; s < numSeries; s++) {
            for (int i = 0; i < lengths[s]; i++) {
                perDocSets[doc++] = sets[s];
            }
        }
        return perDocSets;
    }

    private static int[] randomSet(int valueCount) {
        final int size = randomIntBetween(1, valueCount);
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

    private static Stats write(Directory dir, int[][] perDocSets, int valueCount, int dataPrefix, int metaPrefix) throws IOException {
        final RunTableSortedSetOrdinalWriter writer = new RunTableSortedSetOrdinalWriter(valueCount);
        for (final int[] set : perDocSets) {
            writer.add(set);
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
            return SortedSetRunTableLayout.encode(writer, data, meta);
        }
    }

    private static Stats encode(Directory dir, RunTableSortedSetOrdinalWriter writer) throws IOException {
        try (
            IndexOutput data = dir.createOutput("data.bin", IOContext.DEFAULT);
            IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)
        ) {
            return SortedSetRunTableLayout.encode(writer, data, meta);
        }
    }

    private void verifyAllDriveModes(Directory dir, int[][] expected, int metaPrefix) throws IOException {
        verifySequential(dir, expected, metaPrefix);
        verifyRandomIncreasing(dir, expected, metaPrefix);
        verifyNextDoc(dir, expected, metaPrefix);
    }

    private SortedNumericDocValues open(Directory dir, int maxDoc, int metaPrefix) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        meta.seek(metaPrefix);
        final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);
        return SortedSetRunTableLayout.open(parsed, data, maxDoc);
    }

    private RunTableSortedSetOrdinalReader.Runs openRuns(Directory dir, int maxDoc, int metaPrefix) throws IOException {
        final IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT);
        final IndexInput data = dir.openInput("data.bin", IOContext.DEFAULT);
        meta.seek(metaPrefix);
        final RunTableSortedSetOrdinalReader.Meta parsed = SortedSetRunTableLayout.readMeta(meta);
        return SortedSetRunTableLayout.openRuns(parsed, data, maxDoc);
    }

    private void assertRunSet(RunTableSortedSetOrdinalReader.Runs runs, int run, int startDoc, int length, int[] expectedOrds) {
        assertEquals("run " + run + " startDoc", startDoc, runs.startDoc(run));
        assertEquals("run " + run + " length", length, runs.length(run));
        assertEquals("run " + run + " ordCount", expectedOrds.length, runs.ordCount(run));
        for (int i = 0; i < expectedOrds.length; i++) {
            assertEquals("run " + run + " ord " + i, expectedOrds[i], runs.ordAt(run, i));
        }
    }

    private static void assertDocSet(SortedNumericDocValues dv, int[] expectedSet) throws IOException {
        assertEquals(expectedSet.length, dv.docValueCount());
        long previous = -1;
        for (int i = 0; i < expectedSet.length; i++) {
            final long ord = dv.nextValue();
            assertEquals(expectedSet[i], ord);
            assertTrue("ords must be strictly ascending", ord > previous);
            previous = ord;
        }
    }

    private void verifySequential(Directory dir, int[][] expected, int metaPrefix) throws IOException {
        final SortedNumericDocValues dv = open(dir, expected.length, metaPrefix);
        for (int doc = 0; doc < expected.length; doc++) {
            assertTrue(dv.advanceExact(doc));
            assertDocSet(dv, expected[doc]);
        }
    }

    private void verifyRandomIncreasing(Directory dir, int[][] expected, int metaPrefix) throws IOException {
        final SortedNumericDocValues dv = open(dir, expected.length, metaPrefix);
        int doc = -1;
        while (true) {
            final int next = doc + 1 + randomIntBetween(0, 25);
            if (next >= expected.length) {
                break;
            }
            doc = next;
            assertTrue(dv.advanceExact(doc));
            assertDocSet(dv, expected[doc]);
        }
    }

    private void verifyNextDoc(Directory dir, int[][] expected, int metaPrefix) throws IOException {
        final SortedNumericDocValues dv = open(dir, expected.length, metaPrefix);
        int expectedDoc = 0;
        int doc;
        while ((doc = dv.nextDoc()) != NO_MORE_DOCS) {
            assertEquals(expectedDoc, doc);
            assertDocSet(dv, expected[doc]);
            expectedDoc++;
        }
        assertEquals(expected.length, expectedDoc);
    }

}
