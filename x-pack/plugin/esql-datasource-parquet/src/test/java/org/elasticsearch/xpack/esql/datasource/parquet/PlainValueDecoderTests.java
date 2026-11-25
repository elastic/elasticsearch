/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

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
        byte[] dest = new byte[decoder.plainPayloadCapacityHint(3)];
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
        byte[] dest = new byte[decoder.plainPayloadCapacityHint(1)];
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
        byte[] dest = new byte[Math.max(decoder.plainPayloadCapacityHint(n), 1)];
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
