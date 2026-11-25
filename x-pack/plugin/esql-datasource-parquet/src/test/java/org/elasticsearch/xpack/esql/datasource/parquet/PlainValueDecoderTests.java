/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link PlainValueDecoder}, focusing on boolean bit-packing correctness
 * when read and skip calls are interleaved with non-byte-aligned counts.
 */
public class PlainValueDecoderTests extends ESTestCase {

    /**
     * Verifies that interleaved skip/read calls with non-multiple-of-8 counts
     * produce the same results as a single contiguous read.
     * This is the pattern triggered by {@code PageColumnReader.readBatchSparse}.
     */
    public void testBooleanSkipReadInterleaving() {
        boolean[] expected = new boolean[20];
        byte[] packed = new byte[3];
        for (int i = 0; i < 20; i++) {
            expected[i] = i % 3 == 0;
            if (expected[i]) {
                packed[i / 8] |= (byte) (1 << (i % 8));
            }
        }

        PlainValueDecoder decoder = new PlainValueDecoder();

        // skip 5 (non-aligned), read 3, skip 7, read 5
        decoder.init(ByteBuffer.wrap(packed));
        boolean[] result = new boolean[8];
        decoder.skipBooleans(5);
        decoder.readBooleans(result, 0, 3);
        assertBoolArrayEquals("read after skip(5)", new boolean[] { expected[5], expected[6], expected[7] }, slice(result, 0, 3));

        decoder.skipBooleans(7);
        decoder.readBooleans(result, 0, 5);
        assertBoolArrayEquals(
            "read after skip(7)",
            new boolean[] { expected[15], expected[16], expected[17], expected[18], expected[19] },
            slice(result, 0, 5)
        );
    }

    /**
     * Tests skip/read where each call consumes exactly 1 boolean at a time,
     * alternating skip(1) and read(1) to exercise every bit position within a byte.
     */
    public void testBooleanSingleBitSteps() {
        byte[] packed = new byte[] { (byte) 0b10101010, (byte) 0b01010101 };
        boolean[] expectedAll = new boolean[16];
        for (int i = 0; i < 16; i++) {
            expectedAll[i] = ((packed[i / 8] >>> (i % 8)) & 1) != 0;
        }

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed));

        boolean[] result = new boolean[1];
        for (int i = 0; i < 16; i++) {
            if (i % 2 == 0) {
                decoder.skipBooleans(1);
            } else {
                decoder.readBooleans(result, 0, 1);
                assertEquals("bit " + i, expectedAll[i], result[0]);
            }
        }
    }

    /**
     * Tests that reading all booleans in one call still works (no regression from
     * the boolBitOffset tracking).
     */
    public void testBooleanContiguousRead() {
        byte[] packed = new byte[] { (byte) 0xFF, (byte) 0x00, (byte) 0xAA };
        boolean[] expectedAll = new boolean[20];
        for (int i = 0; i < 20; i++) {
            expectedAll[i] = ((packed[i / 8] >>> (i % 8)) & 1) != 0;
        }

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed));

        boolean[] result = new boolean[20];
        decoder.readBooleans(result, 0, 20);
        assertBoolArrayEquals("contiguous read", expectedAll, result);
    }

    /**
     * Tests that zero-count read/skip calls do not disturb the bit offset.
     */
    public void testBooleanZeroCount() {
        byte[] packed = new byte[] { (byte) 0b10101010 };
        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed));

        decoder.skipBooleans(0);
        decoder.readBooleans(new boolean[0], 0, 0);
        decoder.skipBooleans(3);
        decoder.readBooleans(new boolean[0], 0, 0);

        boolean[] result = new boolean[1];
        decoder.readBooleans(result, 0, 1);
        boolean expected = ((packed[0] >>> 3) & 1) != 0;
        assertEquals("bit 3 after zero-count calls", expected, result[0]);
    }

    /**
     * Tests skip that spans multiple bytes with a non-aligned start.
     */
    public void testBooleanSkipAcrossMultipleBytes() {
        byte[] packed = new byte[] { (byte) 0b11111111, (byte) 0b11111111, (byte) 0b00000001 };
        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed));

        decoder.skipBooleans(3);
        decoder.skipBooleans(13);

        boolean[] result = new boolean[1];
        decoder.readBooleans(result, 0, 1);
        assertTrue("bit 16 should be true", result[0]);
    }

    public void testCopyPlainPayloadsStripsPrefixes() {
        byte[] shortOk = new byte[3];
        Arrays.fill(shortOk, (byte) 'a');
        byte[] longOk = new byte[128];
        Arrays.fill(longOk, (byte) 'b');
        byte[][] payloads = { shortOk, longOk, shortOk };
        byte[] packed = packPlain(payloads);

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN));
        byte[] dest = new byte[decoder.plainPayloadBytes(3)];
        int[] ends = new int[3];
        int destLen = decoder.copyPlainPayloads(dest, 0, ends, 3);
        assertEquals(shortOk.length + longOk.length + shortOk.length, destLen);
        assertEquals(shortOk.length, ends[0]);
        assertEquals(shortOk.length + longOk.length, ends[1]);
        assertEquals(destLen, ends[2]);
        assertArrayEquals(shortOk, Arrays.copyOfRange(dest, 0, ends[0]));
        assertArrayEquals(longOk, Arrays.copyOfRange(dest, ends[0], ends[1]));
        assertArrayEquals(shortOk, Arrays.copyOfRange(dest, ends[1], ends[2]));
    }

    public void testCopyPlainPayloadsDirectBuffer() {
        byte[] payload = { 'a', 'b' };
        byte[] packed = packPlain(new byte[][] { payload });
        ByteBuffer direct = ByteBuffer.allocateDirect(packed.length).order(ByteOrder.LITTLE_ENDIAN);
        direct.put(packed).flip();

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(direct);
        byte[] dest = new byte[decoder.plainPayloadBytes(1)];
        int[] ends = new int[1];
        int destLen = decoder.copyPlainPayloads(dest, 0, ends, 1);
        assertEquals(2, destLen);
        assertEquals(2, ends[0]);
        assertArrayEquals(payload, Arrays.copyOf(dest, destLen));
    }

    public void testCopyPlainPayloadsRandomLengthsIncludingEmpty() {
        int n = randomIntBetween(1, 32);
        byte[][] payloads = new byte[n][];
        int expectedLen = 0;
        for (int i = 0; i < n; i++) {
            int len = switch (randomIntBetween(0, 4)) {
                case 0 -> randomIntBetween(0, 127);
                case 1 -> randomIntBetween(128, 255);
                case 2 -> randomIntBetween(256, 383);
                case 3 -> randomIntBetween(384, 512);
                default -> 0;
            };
            payloads[i] = randomByteArrayOfLength(len);
            expectedLen += len;
        }
        byte[] packed = packPlain(payloads);
        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN));
        byte[] dest = new byte[decoder.plainPayloadBytes(n)];
        int[] ends = new int[n];
        int destLen = decoder.copyPlainPayloads(dest, 0, ends, n);
        assertEquals(expectedLen, destLen);
        int start = 0;
        for (int i = 0; i < n; i++) {
            assertEquals(start + payloads[i].length, ends[i]);
            assertArrayEquals(payloads[i], Arrays.copyOfRange(dest, start, ends[i]));
            start = ends[i];
        }
    }

    /**
     * {@code plainPayloadBytes(k)} is the exact payload sum of the next {@code k} values, not the page
     * remainder, and it leaves the position alone: a following {@code copyPlainPayloads} still starts at
     * the first value and writes exactly that many bytes, and the next call counts from where the copy stopped.
     */
    public void testPlainPayloadBytesIsExactSumAndKeepsPosition() {
        int n = randomIntBetween(2, 32);
        byte[][] payloads = new byte[n][];
        for (int i = 0; i < n; i++) {
            payloads[i] = randomByteArrayOfLength(randomFrom(0, randomIntBetween(1, 300)));
        }
        int first = randomIntBetween(1, n - 1);
        int firstBytes = 0;
        for (int i = 0; i < first; i++) {
            firstBytes += payloads[i].length;
        }
        int restBytes = 0;
        for (int i = first; i < n; i++) {
            restBytes += payloads[i].length;
        }

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(randomPlainBuffer(packPlain(payloads)));
        assertEquals(0, decoder.plainPayloadBytes(0));
        assertEquals(firstBytes, decoder.plainPayloadBytes(first));
        assertEquals("repeat call must see the same position", firstBytes, decoder.plainPayloadBytes(first));

        byte[] dest = new byte[firstBytes];
        int[] ends = new int[first];
        assertEquals(firstBytes, decoder.copyPlainPayloads(dest, 0, ends, first));
        assertArrayEquals(payloads[0], Arrays.copyOf(dest, ends[0]));

        assertEquals(restBytes, decoder.plainPayloadBytes(n - first));
    }

    /** A length prefix cut short fails with the {@link BufferUnderflowException} {@code copyPlainPayloads} throws. */
    public void testPlainPayloadBytesTruncatedPrefix() {
        byte[] full = packPlain(new byte[][] { new byte[] { 'a' }, new byte[] { 'b' } });
        byte[] truncated = Arrays.copyOf(full, full.length - 1 - randomIntBetween(1, Integer.BYTES - 1));

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(randomPlainBuffer(truncated));
        expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(2));
        expectThrows(BufferUnderflowException.class, () -> decoder.copyPlainPayloads(new byte[2], 0, new int[2], 2));
    }

    /**
     * A payload running past the buffer limit fails with {@link BufferUnderflowException}, as
     * {@code copyPlainPayloads} does over a direct buffer.
     */
    public void testPlainPayloadBytesTruncatedPayload() {
        byte[] full = packPlain(new byte[][] { new byte[] { 'a' }, new byte[] { 'b', 'c', 'd' } });
        byte[] truncated = Arrays.copyOf(full, full.length - randomIntBetween(1, 3));

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(directBuffer(truncated));
        expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(2));
        expectThrows(BufferUnderflowException.class, () -> decoder.copyPlainPayloads(new byte[4], 0, new int[2], 2));

        decoder.init(ByteBuffer.wrap(truncated));
        expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(2));
    }

    /**
     * A negative length can only come from a corrupt page; both methods count it as an empty value that consumes
     * just its 4-byte prefix, so they agree on the size and the values after it decode unchanged.
     */
    public void testPlainPayloadBytesNegativeLengthCountsAsEmpty() {
        byte[] packed = packPlain(new byte[][] { new byte[] { 'a' }, new byte[0], new byte[] { 'c', 'd' } });
        int secondPrefix = Integer.BYTES + 1;
        ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN).putInt(secondPrefix, -randomIntBetween(1, Integer.MAX_VALUE));

        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(randomPlainBuffer(packed));
        assertEquals(1, decoder.plainPayloadBytes(1));
        assertEquals("the negative length adds no payload bytes", 1, decoder.plainPayloadBytes(2));
        int expected = decoder.plainPayloadBytes(3);
        assertEquals(3, expected);

        byte[] dest = new byte[expected];
        int[] ends = new int[3];
        assertEquals("copyPlainPayloads must agree with plainPayloadBytes", expected, decoder.copyPlainPayloads(dest, 0, ends, 3));
        assertArrayEquals("the negative length is an empty value", new int[] { 1, 1, 3 }, ends);
        assertArrayEquals("the value after it decodes unchanged", new byte[] { 'a', 'c', 'd' }, dest);
        // Every value, and nothing more, was consumed: the buffer is exhausted.
        expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(1));
    }

    /**
     * A negative length in the middle of a page reads as an empty value that consumes only its 4-byte prefix,
     * over both a heap and a direct buffer: the values after it decode unchanged and nothing is left over.
     */
    public void testReadBinariesNegativeLengthIsEmpty() {
        byte[][] payloads = { new byte[] { 'a' }, new byte[0], new byte[] { 'c', 'd' } };
        byte[] packed = packPlainWithNegativeLengths(payloads, new boolean[] { false, true, false });
        for (ByteBuffer buffer : List.of(ByteBuffer.wrap(packed), directBuffer(packed))) {
            PlainValueDecoder decoder = new PlainValueDecoder();
            decoder.init(buffer);
            BytesRef[] vals = new BytesRef[3];
            decoder.readBinaries(vals, 0, 3);
            assertEquals(new BytesRef("a"), vals[0]);
            assertEquals("the negative length is an empty value", new BytesRef(), vals[1]);
            assertEquals("the value after it decodes unchanged", new BytesRef("cd"), vals[2]);
            expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(1));
        }
    }

    /** Skipping over a negative length consumes only its prefix, so the next read lands on the following value. */
    public void testSkipBinariesNegativeLengthSkipsOnlyPrefix() {
        byte[][] payloads = { new byte[] { 'a' }, new byte[0], new byte[] { 'c', 'd' }, new byte[] { 'e' } };
        byte[] packed = packPlainWithNegativeLengths(payloads, new boolean[] { false, true, false, false });
        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(randomPlainBuffer(packed));
        decoder.skipBinaries(2);
        BytesRef[] vals = new BytesRef[2];
        decoder.readBinaries(vals, 0, 2);
        assertEquals(new BytesRef("cd"), vals[0]);
        assertEquals(new BytesRef("e"), vals[1]);
        expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(1));
    }

    /**
     * Every length-prefixed path agrees on a page with random negative lengths: {@code readBinaries} over the
     * whole page, {@code skipBinaries} then {@code readBinaries}, and {@code plainPayloadBytes} plus
     * {@code copyPlainPayloads} in two chunks all see the same values and all end exactly at the buffer end.
     */
    public void testNegativeLengthReadSkipAndCopyAgree() {
        int n = randomIntBetween(2, 32);
        byte[][] payloads = new byte[n][];
        boolean[] negative = new boolean[n];
        for (int i = 0; i < n; i++) {
            negative[i] = randomBoolean();
            payloads[i] = negative[i] ? new byte[0] : randomByteArrayOfLength(randomIntBetween(0, 300));
        }
        byte[] packed = packPlainWithNegativeLengths(payloads, negative);
        int split = randomIntBetween(0, n);

        PlainValueDecoder read = new PlainValueDecoder();
        read.init(randomPlainBuffer(packed));
        BytesRef[] all = new BytesRef[n];
        read.readBinaries(all, 0, n);
        for (int i = 0; i < n; i++) {
            assertEquals("readBinaries value " + i, new BytesRef(payloads[i]), all[i]);
        }

        PlainValueDecoder skipThenRead = new PlainValueDecoder();
        skipThenRead.init(randomPlainBuffer(packed));
        skipThenRead.skipBinaries(split);
        BytesRef[] rest = new BytesRef[n - split];
        skipThenRead.readBinaries(rest, 0, n - split);
        for (int i = 0; i < n - split; i++) {
            assertEquals("skip+read value " + (split + i), new BytesRef(payloads[split + i]), rest[i]);
        }

        PlainValueDecoder copy = new PlainValueDecoder();
        copy.init(randomPlainBuffer(packed));
        for (int[] chunk : new int[][] { { 0, split }, { split, n } }) {
            int count = chunk[1] - chunk[0];
            byte[] dest = new byte[copy.plainPayloadBytes(count)];
            int[] ends = new int[count];
            assertEquals(dest.length, copy.copyPlainPayloads(dest, 0, ends, count));
            int start = 0;
            for (int i = 0; i < count; i++) {
                assertArrayEquals("copy value " + (chunk[0] + i), payloads[chunk[0] + i], Arrays.copyOfRange(dest, start, ends[i]));
                start = ends[i];
            }
        }

        for (PlainValueDecoder decoder : List.of(read, skipThenRead, copy)) {
            expectThrows(BufferUnderflowException.class, () -> decoder.plainPayloadBytes(1));
        }
    }

    public void testReadBinariesNonZeroOffset() {
        byte[] packed = packPlain(new byte[][] { new byte[] { 'x' }, new byte[] { 'y' } });
        PlainValueDecoder decoder = new PlainValueDecoder();
        decoder.init(ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN));
        BytesRef[] vals = new BytesRef[4];
        decoder.readBinaries(vals, 1, 2);
        assertEquals(new BytesRef(new byte[] { 'x' }), vals[1]);
        assertEquals(new BytesRef(new byte[] { 'y' }), vals[2]);
    }

    private static byte[] packPlain(byte[][] payloads) {
        int len = 0;
        for (byte[] p : payloads) {
            len += Integer.BYTES + p.length;
        }
        byte[] packed = new byte[len];
        int pos = 0;
        for (byte[] p : payloads) {
            int n = p.length;
            packed[pos] = (byte) n;
            packed[pos + 1] = (byte) (n >>> 8);
            packed[pos + 2] = (byte) (n >>> 16);
            packed[pos + 3] = (byte) (n >>> 24);
            pos += Integer.BYTES;
            System.arraycopy(p, 0, packed, pos, p.length);
            pos += p.length;
        }
        return packed;
    }

    /**
     * Packs {@code payloads} as PLAIN BINARY, then overwrites the length prefix of every value flagged in
     * {@code negative} with a random negative length, as a corrupt page would hold. Flagged values must be
     * empty so the bytes after the rewritten prefix are still the next value's prefix.
     */
    private static byte[] packPlainWithNegativeLengths(byte[][] payloads, boolean[] negative) {
        byte[] packed = packPlain(payloads);
        ByteBuffer view = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN);
        int pos = 0;
        for (int i = 0; i < payloads.length; i++) {
            if (negative[i]) {
                assertEquals("only an empty value can carry a negative length", 0, payloads[i].length);
                view.putInt(pos, -randomIntBetween(1, Integer.MAX_VALUE));
            }
            pos += Integer.BYTES + payloads[i].length;
        }
        return packed;
    }

    private static ByteBuffer randomPlainBuffer(byte[] packed) {
        return randomBoolean() ? ByteBuffer.wrap(packed) : directBuffer(packed);
    }

    private static ByteBuffer directBuffer(byte[] packed) {
        ByteBuffer direct = ByteBuffer.allocateDirect(packed.length);
        direct.put(packed).flip();
        return direct;
    }

    private static boolean[] slice(boolean[] arr, int offset, int length) {
        boolean[] result = new boolean[length];
        System.arraycopy(arr, offset, result, 0, length);
        return result;
    }

    private static void assertBoolArrayEquals(String msg, boolean[] expected, boolean[] actual) {
        assertEquals(msg + " length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(msg + " [" + i + "]", expected[i], actual[i]);
        }
    }
}
