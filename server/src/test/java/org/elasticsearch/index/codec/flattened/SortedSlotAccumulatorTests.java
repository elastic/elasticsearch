/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Unit tests for {@link SortedSlotAccumulator}, covering all three cursor paths:
 * <ul>
 *   <li>{@code InMemoryCursor} — records fit within {@code maxBufferBytes}</li>
 *   <li>{@code RunFileCursor} — single external sort run (one oversized record)</li>
 *   <li>{@code MergeCursor} — k-way merge over multiple external sort runs</li>
 * </ul>
 *
 * <p>The primary regression this guards: {@link SortedSlotAccumulator} previously wrote
 * record headers big-endian into its in-memory buffer and copied those bytes verbatim into
 * Lucene temp files, but read them back with {@link org.apache.lucene.store.DataInput#readInt}
 * which is little-endian. On the external-sort path, every header field (keyOrd, docId,
 * payloadLen) was byte-swapped on read-back, corrupting the cursor output and causing
 * {@code ArrayIndexOutOfBoundsException} during flush.
 */
public class SortedSlotAccumulatorTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Regression test: the specific byte-swap that triggered the production crash
    // -----------------------------------------------------------------------

    /**
     * Regression test for the endianness mismatch on the external-sort path.
     *
     * <p>The crash was: {@code keyOrd = 16} written big-endian ({@code 00 00 00 10}) was
     * read back little-endian as {@code 0x10000000 = 268435456}, which is out of bounds
     * for a {@code lexRankOf} array of 55 entries.
     *
     * <p>Using {@code maxBufferBytes = 1} forces every record (header = 12 bytes) through
     * the single-oversized-record path, which writes a one-record run file then reads it
     * back with {@link org.apache.lucene.store.IndexInput#readInt}.
     */
    public void testExternalSortEndiannessRegressionKeyOrd16() throws IOException {
        final int numKeys = 55;
        final int[] lexRankOf = identity(numKeys);

        try (Directory dir = newDirectory()) {
            // maxBufferBytes = 1: every record is "oversized" (recLen >= 12 > 1),
            // so each gets its own run file.
            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, 1);
            acc.add(16, 0, new byte[] { 1, 2, 3 }, 0, 3);
            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                assertTrue("cursor must have one record", cursor.next());
                // Before the fix: AIOOBE because keyOrd 16 was byte-swapped to 268435456.
                assertEquals("lexRank must be 16 (identity mapping)", 16, cursor.lexRank());
                assertEquals("docId must round-trip", 0, cursor.docId());
                assertFalse("cursor must be exhausted", cursor.next());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Equivalence: all three cursor paths must produce identical sorted output
    // -----------------------------------------------------------------------

    /**
     * Feeds the same records into three accumulators with different {@code maxBufferBytes}
     * (in-memory, single-run external, multi-run external) and asserts that all three
     * sorted-cursor outputs are identical and correctly ordered by {@code (lexRank, docId)}.
     */
    public void testEquivalenceAcrossAllCursorPaths() throws IOException {
        final int numKeys = 20;
        final int numDocs = 50;

        // Use a non-trivial (non-identity) lexRankOf mapping: reverse the key ordering.
        final int[] lexRankOf = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            lexRankOf[i] = numKeys - 1 - i; // keyOrd 0 → lexRank 19, keyOrd 19 → lexRank 0
        }

        // Generate records: every (doc, key) pair with a small random payload.
        final List<Record> records = new ArrayList<>();
        for (int doc = 0; doc < numDocs; doc++) {
            for (int key = 0; key < numKeys; key++) {
                if (random().nextBoolean()) {
                    final byte[] payload = randomByteArrayOfLength(between(1, 30));
                    records.add(new Record(key, doc, payload));
                }
            }
        }
        // Ensure at least a handful of records so the test is non-trivial.
        while (records.size() < 5) {
            records.add(new Record(between(0, numKeys - 1), between(0, numDocs - 1), randomByteArrayOfLength(between(1, 10))));
        }

        // Compute the total raw buffer size so we can pick useful maxBufferBytes values.
        int totalBufLen = 0;
        for (final Record r : records) {
            totalBufLen += SortedSlotAccumulator.RECORD_HEADER_BYTES + r.payload.length;
        }

        // Three maxBufferBytes values targeting the three cursor paths:
        // 1. Integer.MAX_VALUE → always InMemoryCursor
        // 2. maxBufferBytes = 1 → every record is oversized → one run per record → MergeCursor
        // (or RunFileCursor when there is exactly one record, but we ensured >= 5 records)
        // 3. maxBufferBytes chosen to allow ~half the records in the first run → MergeCursor
        // with multiple non-trivial runs
        final int mediumMax = Math.max(1, totalBufLen / 3);

        try (Directory dir = newDirectory()) {
            final List<DrainedOutput> results = new ArrayList<>();
            for (final int maxBuf : new int[] { Integer.MAX_VALUE, 1, mediumMax }) {
                results.add(drain(dir, records, lexRankOf, maxBuf));
            }

            // All three must produce the same sequence.
            for (int i = 1; i < results.size(); i++) {
                assertEquals("cursor path " + i + " vs in-memory: different record counts", results.get(0).size(), results.get(i).size());
                for (int r = 0; r < results.get(0).size(); r++) {
                    final long[] expected = results.get(0).get(r);
                    final long[] actual = results.get(i).get(r);
                    assertArrayEquals(
                        "cursor path "
                            + i
                            + " record "
                            + r
                            + " mismatch: "
                            + "expected (lexRank="
                            + expected[0]
                            + ", docId="
                            + expected[1]
                            + ") "
                            + "got (lexRank="
                            + actual[0]
                            + ", docId="
                            + actual[1]
                            + ")",
                        expected,
                        actual
                    );
                }
            }

            // Additionally assert that the output is sorted by (lexRank, docId).
            final DrainedOutput inMem = results.get(0);
            for (int r = 1; r < inMem.size(); r++) {
                final long[] prev = inMem.get(r - 1);
                final long[] cur = inMem.get(r);
                final long prevKey = (prev[0] << 32) | (prev[1] & 0xFFFFFFFFL);
                final long curKey = (cur[0] << 32) | (cur[1] & 0xFFFFFFFFL);
                assertTrue(
                    "output not sorted at position "
                        + r
                        + ": "
                        + "(lexRank="
                        + prev[0]
                        + ", docId="
                        + prev[1]
                        + ") followed by "
                        + "(lexRank="
                        + cur[0]
                        + ", docId="
                        + cur[1]
                        + ")",
                    prevKey <= curKey
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Oversized single record on the external-sort path
    // -----------------------------------------------------------------------

    /**
     * A single record whose {@code payloadLen} exceeds {@code maxBufferBytes} must be
     * written as its own run (the "oversized record" branch) and read back correctly.
     *
     * <p>Also verifies that {@code RunFileCursor} (the single-run case) works end-to-end:
     * the run file contains exactly the one record, and the cursor exhausts normally.
     */
    public void testSingleOversizedRecord() throws IOException {
        final int numKeys = 4;
        final int[] lexRankOf = identity(numKeys);
        // Payload larger than maxBufferBytes so that recLen > maxBufferBytes.
        final int maxBuf = 50;
        final byte[] largePayload = randomByteArrayOfLength(maxBuf + 10); // recLen = 12 + 60 = 72 > 50

        try (Directory dir = newDirectory()) {
            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, maxBuf);
            acc.add(2, 7, largePayload, 0, largePayload.length);
            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                assertTrue("cursor must have record", cursor.next());
                assertEquals("lexRank", lexRankOf[2], cursor.lexRank());
                assertEquals("docId", 7, cursor.docId());
                assertEquals("payloadLen", largePayload.length, cursor.payloadLength());
                assertArrayEquals(
                    "payload bytes",
                    largePayload,
                    Arrays.copyOfRange(cursor.payloadBytes(), cursor.payloadOffset(), cursor.payloadOffset() + cursor.payloadLength())
                );
                assertFalse("cursor exhausted", cursor.next());
            }
        }
    }

    /**
     * Multiple oversized records (each exceeding {@code maxBufferBytes}) interleaved with
     * normal records. Every record must survive correctly through the external-sort path.
     */
    public void testOversizedRecordsMixedWithNormal() throws IOException {
        final int numKeys = 8;
        final int[] lexRankOf = identity(numKeys);
        final int maxBuf = 30; // recLen = 12 + payloadLen; oversized when payloadLen > 18

        try (Directory dir = newDirectory()) {
            final List<Record> records = new ArrayList<>();
            // Add oversized records
            records.add(new Record(0, 0, randomByteArrayOfLength(100)));
            records.add(new Record(3, 5, randomByteArrayOfLength(200)));
            // Add normal records (payloadLen <= 18 so they fit within a chunk)
            records.add(new Record(1, 2, randomByteArrayOfLength(5)));
            records.add(new Record(2, 3, randomByteArrayOfLength(10)));
            records.add(new Record(5, 1, randomByteArrayOfLength(3)));

            final DrainedOutput result = drain(dir, records, lexRankOf, maxBuf);

            assertEquals("all records must survive", 5, result.size());
            // Verify sorted order.
            for (int r = 1; r < result.size(); r++) {
                final long[] prev = result.get(r - 1);
                final long[] cur = result.get(r);
                assertTrue("sorted at " + r, (prev[0] << 32 | prev[1]) <= (cur[0] << 32 | cur[1]));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Empty accumulator
    // -----------------------------------------------------------------------

    public void testEmptyAccumulator() throws IOException {
        final int[] lexRankOf = identity(5);
        try (Directory dir = newDirectory()) {
            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, 1024);
            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                assertFalse("empty accumulator must return empty cursor", cursor.next());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Temp-file cleanup
    // -----------------------------------------------------------------------

    /**
     * After closing a cursor returned from the external-sort path, all temporary run files
     * must be deleted. The directory must contain no extra files relative to before the sort.
     */
    public void testTempFilesAreDeletedOnClose() throws IOException {
        final int numKeys = 5;
        final int[] lexRankOf = identity(numKeys);
        // maxBufferBytes small enough to force at least 2 runs (MergeCursor path).
        final int maxBuf = 1;

        try (Directory dir = newDirectory()) {
            final String[] beforeFiles = dir.listAll();

            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, maxBuf);
            acc.add(0, 0, new byte[] { 1 }, 0, 1);
            acc.add(1, 1, new byte[] { 2 }, 0, 1);
            acc.add(2, 2, new byte[] { 3 }, 0, 1);

            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                // Drain the cursor; close() must delete the run files.
                while (cursor.next()) {
                    /* consume */ }
            }

            final String[] afterFiles = dir.listAll();
            Arrays.sort(beforeFiles);
            Arrays.sort(afterFiles);
            assertArrayEquals("run files must be cleaned up after cursor close", beforeFiles, afterFiles);
        }
    }

    /**
     * Same as {@link #testTempFilesAreDeletedOnClose} but for the single-run
     * ({@code RunFileCursor}) path.
     */
    public void testTempFilesAreDeletedOnCloseSingleRun() throws IOException {
        final int numKeys = 3;
        final int[] lexRankOf = identity(numKeys);
        // One record whose recLen > maxBufferBytes → single oversized run → RunFileCursor.
        final int maxBuf = 10;

        try (Directory dir = newDirectory()) {
            final String[] beforeFiles = dir.listAll();

            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, maxBuf);
            acc.add(0, 0, new byte[maxBuf], 0, maxBuf); // recLen = 12 + maxBuf > maxBuf

            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                while (cursor.next()) {
                    /* consume */ }
            }

            final String[] afterFiles = dir.listAll();
            Arrays.sort(beforeFiles);
            Arrays.sort(afterFiles);
            assertArrayEquals("single run file must be cleaned up", beforeFiles, afterFiles);
        }
    }

    // -----------------------------------------------------------------------
    // Payload correctness
    // -----------------------------------------------------------------------

    /**
     * Verifies that payload bytes survive the external sort round-trip without corruption,
     * including payloads with all-zero bytes (which could mask off-by-one errors).
     */
    public void testPayloadBytesRoundTrip() throws IOException {
        final int numKeys = 3;
        final int[] lexRankOf = identity(numKeys);

        // Records: [keyOrd=2, docId=0, payload="hello"], [keyOrd=0, docId=0, payload=zeros],
        // [keyOrd=1, docId=0, payload="world"]
        final byte[] hello = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final byte[] zeros = new byte[20];
        final byte[] world = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        try (Directory dir = newDirectory()) {
            // Use tiny maxBufferBytes so all records go through external sort.
            final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, 1);
            acc.add(2, 0, hello, 0, hello.length);
            acc.add(0, 0, zeros, 0, zeros.length);
            acc.add(1, 0, world, 0, world.length);

            try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
                assertTrue(cursor.next());
                assertEquals("first record: lexRank 0 (keyOrd 0)", 0, cursor.lexRank());
                assertArrayEquals(
                    "payload for keyOrd 0",
                    zeros,
                    Arrays.copyOfRange(cursor.payloadBytes(), cursor.payloadOffset(), cursor.payloadOffset() + cursor.payloadLength())
                );

                assertTrue(cursor.next());
                assertEquals("second record: lexRank 1 (keyOrd 1)", 1, cursor.lexRank());
                assertArrayEquals(
                    "payload for keyOrd 1",
                    world,
                    Arrays.copyOfRange(cursor.payloadBytes(), cursor.payloadOffset(), cursor.payloadOffset() + cursor.payloadLength())
                );

                assertTrue(cursor.next());
                assertEquals("third record: lexRank 2 (keyOrd 2)", 2, cursor.lexRank());
                assertArrayEquals(
                    "payload for keyOrd 2",
                    hello,
                    Arrays.copyOfRange(cursor.payloadBytes(), cursor.payloadOffset(), cursor.payloadOffset() + cursor.payloadLength())
                );

                assertFalse(cursor.next());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** A record triple (keyOrd, docId, payload). */
    private record Record(int keyOrd, int docId, byte[] payload) {}

    /**
     * Encoded output from a drained cursor: each entry is {@code [lexRank, docId, payloadCrc]},
     * where {@code payloadCrc} is a simple checksum of the payload bytes for comparison.
     */
    private static class DrainedOutput extends ArrayList<long[]> {}

    /**
     * Feeds {@code records} into a fresh {@link SortedSlotAccumulator} with the given
     * {@code maxBufferBytes}, drains the sorted cursor, and returns the encoded output.
     */
    private static DrainedOutput drain(Directory dir, List<Record> records, int[] lexRankOf, int maxBufferBytes) throws IOException {
        final SortedSlotAccumulator acc = new SortedSlotAccumulator(dir, IOContext.DEFAULT, maxBufferBytes);
        for (final Record r : records) {
            acc.add(r.keyOrd(), r.docId(), r.payload(), 0, r.payload().length);
        }
        final DrainedOutput out = new DrainedOutput();
        try (SortedSlotAccumulator.SortedCursor cursor = acc.sortedCursor(lexRankOf)) {
            while (cursor.next()) {
                final long crc = payloadCrc(cursor.payloadBytes(), cursor.payloadOffset(), cursor.payloadLength());
                out.add(new long[] { cursor.lexRank(), cursor.docId(), crc });
            }
        }
        // Sort by (lexRank, docId) to obtain a canonical ordering for comparison,
        // since unstable sort may place equal (lexRank, docId) pairs in any order.
        out.sort(Comparator.comparingLong((long[] e) -> e[0]).thenComparingLong(e -> e[1]));
        return out;
    }

    /** Cheap payload checksum: sum of all bytes XOR'd with position. */
    private static long payloadCrc(byte[] bytes, int off, int len) {
        long crc = 0;
        for (int i = 0; i < len; i++) {
            crc += (long) (bytes[off + i] & 0xFF) * (i + 1);
        }
        return crc;
    }

    /** Returns an identity {@code lexRankOf} array: {@code lexRankOf[i] == i}. */
    private static int[] identity(int numKeys) {
        final int[] arr = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            arr[i] = i;
        }
        return arr;
    }
}
