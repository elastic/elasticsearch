/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate.internal;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * These encodings are on-disk formats read back by Lucene's {@code DataInput}, so the tests that matter compare
 * against Lucene rather than only round-tripping against themselves: a self-consistent but divergent vint would
 * pass a round trip and corrupt every segment.
 */
public class ByteArrayIntsTests extends ESTestCase {

    /** Values either side of every vint width boundary, where an off-by-one in the shift loop would show. */
    private static final int[] INT_BOUNDARIES = {
        0,
        1,
        0x7F,
        0x80,
        0x3FFF,
        0x4000,
        0x1FFFFF,
        0x200000,
        0xFFFFFFF,
        0x10000000,
        Integer.MAX_VALUE };

    private static final long[] LONG_BOUNDARIES = {
        0L,
        1L,
        0x7FL,
        0x80L,
        0x3FFFL,
        0x4000L,
        0xFFFFFFFFL,
        0x7FFFFFFFFFFFFFL,
        0x80000000000000L,
        Long.MAX_VALUE };

    public void testVIntMatchesLucene() throws IOException {
        for (int value : INT_BOUNDARIES) {
            assertVIntMatchesLucene(value);
        }
        for (int i = 0; i < 1000; i++) {
            assertVIntMatchesLucene(randomNonNegativeInt());
        }
    }

    public void testVLongMatchesLucene() throws IOException {
        for (long value : LONG_BOUNDARIES) {
            assertVLongMatchesLucene(value);
        }
        for (int i = 0; i < 1000; i++) {
            assertVLongMatchesLucene(randomNonNegativeLong());
        }
    }

    /** The decode side has to accept what Lucene writes, not merely what this class writes. */
    public void testReadVIntAcceptsLuceneOutput() throws IOException {
        for (int value : INT_BOUNDARIES) {
            final byte[] buffer = new byte[ByteArrayInts.MAX_VINT_BYTES];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            out.writeVInt(value);
            assertEquals("value " + value, value, ByteArrayInts.readVInt(buffer, 0));
            assertEquals("width of " + value, out.getPosition(), ByteArrayInts.vIntLength(value));
        }
    }

    /** A vint is written at an offset inside a shared scratch buffer, so it must not assume it starts at zero. */
    public void testVIntAtAnOffset() throws IOException {
        final byte[] buffer = new byte[64];
        final int offset = between(1, 32);
        final int value = randomNonNegativeInt();
        final int written = ByteArrayInts.writeVInt(value, buffer, offset);
        assertEquals("bytes written", ByteArrayInts.vIntLength(value), written);
        assertEquals("value", value, ByteArrayInts.readVInt(buffer, offset));
        assertEquals("nothing written before the offset", 0, buffer[offset - 1]);
    }

    /**
     * A vint with a continuation bit on the fifth byte is malformed; both overloads must throw rather than
     * run off the array or silently return a wrong value.
     */
    public void testReadVIntRejectsMalformedInput() {
        // All five bytes have the continuation bit set — no valid vint, ever.
        final byte[] bad = new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0 };
        assertThrows(IOException.class, () -> ByteArrayInts.readVInt(bad, 0));
        assertThrows(IOException.class, () -> ByteArrayInts.readVInt(bad, new int[1]));
    }

    /**
     * The cursor overload reads the same value as the offset overload and leaves the cursor at the next
     * position, so callers in a decode loop get the advance without a separate {@code vIntLength} call.
     */
    public void testCursorOverloadAdvancesPosition() throws IOException {
        final byte[] buffer = new byte[ByteArrayInts.MAX_VINT_BYTES * 3];
        final int[] values = new int[3];
        int written = 0;
        for (int i = 0; i < 3; i++) {
            values[i] = INT_BOUNDARIES[between(0, INT_BOUNDARIES.length - 1)];
            written += ByteArrayInts.writeVInt(values[i], buffer, written);
        }
        final int[] pos = new int[1];
        for (int i = 0; i < 3; i++) {
            final int before = pos[0];
            final int value = ByteArrayInts.readVInt(buffer, pos);
            assertEquals("value " + i, values[i], value);
            assertEquals("advance " + i, ByteArrayInts.vIntLength(values[i]), pos[0] - before);
        }
    }

    /** Lengths are decoded by stepping over each vint, so a wrong width silently shifts every later value. */
    public void testVIntLengthTracksWhatWasWritten() {
        final byte[] buffer = new byte[ByteArrayInts.MAX_VINT_BYTES];
        for (int i = 0; i < 2000; i++) {
            final int value = randomBoolean() ? INT_BOUNDARIES[between(0, INT_BOUNDARIES.length - 1)] : randomNonNegativeInt();
            assertEquals("width of " + value, ByteArrayInts.writeVInt(value, buffer, 0), ByteArrayInts.vIntLength(value));
        }
    }

    public void testVIntNeverExceedsItsMaximum() {
        assertEquals(ByteArrayInts.MAX_VINT_BYTES, ByteArrayInts.vIntLength(Integer.MAX_VALUE));
        final byte[] buffer = new byte[ByteArrayInts.MAX_VINT_BYTES];
        assertEquals(ByteArrayInts.MAX_VINT_BYTES, ByteArrayInts.writeVInt(Integer.MAX_VALUE, buffer, 0));
    }

    /**
     * A negative vlong takes the full ten bytes. Callers assert non-negative, but the buffer they size with
     * {@link ByteArrayInts#MAX_VLONG_BYTES} has to hold one regardless, since an assertion is disabled in
     * production.
     */
    public void testVLongOfNegativeFitsItsMaximum() {
        final byte[] buffer = new byte[ByteArrayInts.MAX_VLONG_BYTES];
        assertEquals(ByteArrayInts.MAX_VLONG_BYTES, ByteArrayInts.writeVLong(-1L, buffer, 0));
        assertEquals(ByteArrayInts.MAX_VLONG_BYTES, ByteArrayInts.writeVLong(Long.MIN_VALUE, buffer, 0));
    }

    public void testWidthForPicksTheNarrowestWidth() {
        assertEquals(1, ByteArrayInts.widthFor(0));
        assertEquals(1, ByteArrayInts.widthFor(0xFF));
        assertEquals(2, ByteArrayInts.widthFor(0x100));
        assertEquals(2, ByteArrayInts.widthFor(0xFFFF));
        assertEquals(4, ByteArrayInts.widthFor(0x10000));
        assertEquals(4, ByteArrayInts.widthFor(Integer.MAX_VALUE));
    }

    public void testFixedWidthRoundTrip() {
        final byte[] buffer = new byte[64];
        for (int width : new int[] { 1, 2, 4 }) {
            final int max = width == 4 ? Integer.MAX_VALUE : (1 << (8 * width)) - 1;
            for (int i = 0; i < 500; i++) {
                final int value = between(0, max);
                assertTrue("width " + width + " must hold " + value, ByteArrayInts.widthFor(value) <= width);
                final int offset = between(0, 8);
                ByteArrayInts.writeIntLE(value, width, buffer, offset);
                assertEquals("width " + width + " value " + value, value, ByteArrayInts.readIntLE(buffer, offset, width));
            }
        }
    }

    /** Little-endian, so the low byte comes first — the order the reader's shift loop assumes. */
    public void testFixedWidthIsLittleEndian() {
        final byte[] buffer = new byte[4];
        ByteArrayInts.writeIntLE(0x04030201, 4, buffer, 0);
        assertEquals(new BytesRef(new byte[] { 1, 2, 3, 4 }), new BytesRef(buffer));
    }

    /** Values written back to back must be independently recoverable, which is how a packed block is read. */
    public void testFixedWidthSequence() {
        for (int width : new int[] { 1, 2, 4 }) {
            final int count = between(1, 128);
            final int max = width == 4 ? Integer.MAX_VALUE : (1 << (8 * width)) - 1;
            final int[] values = new int[count];
            final byte[] buffer = new byte[count * width];
            for (int i = 0; i < count; i++) {
                values[i] = between(0, max);
                ByteArrayInts.writeIntLE(values[i], width, buffer, i * width);
            }
            for (int i = 0; i < count; i++) {
                assertEquals("width " + width + " at " + i, values[i], ByteArrayInts.readIntLE(buffer, i * width, width));
            }
        }
    }

    /** Zero max maps to zero bits, non-zero max maps to the narrowest count that fits. */
    public void testBitsRequired() {
        assertEquals(0, ByteArrayInts.bitsRequired(0));
        assertEquals(1, ByteArrayInts.bitsRequired(1));
        assertEquals(4, ByteArrayInts.bitsRequired(15));
        assertEquals(5, ByteArrayInts.bitsRequired(16));
        assertEquals(8, ByteArrayInts.bitsRequired(255));
        assertEquals(9, ByteArrayInts.bitsRequired(256));
        assertEquals(17, ByteArrayInts.bitsRequired(65537));
        assertEquals(31, ByteArrayInts.bitsRequired(Integer.MAX_VALUE));
        for (int i = 0; i < 1000; i++) {
            final int v = randomNonNegativeInt();
            final int bits = ByteArrayInts.bitsRequired(v);
            assertTrue("bits=" + bits + " must hold " + v, bits == 0 || (v >> (bits - 1)) > 0);
            if (bits < 31) {
                assertTrue("bits=" + bits + " is not more than needed for " + v, (v >> bits) == 0);
            }
        }
    }

    /** The packed byte count matches what the write actually uses. */
    public void testBitPackedLength() {
        assertEquals(0, ByteArrayInts.bitPackedLength(128, 0));
        assertEquals(16, ByteArrayInts.bitPackedLength(128, 1));
        assertEquals(64, ByteArrayInts.bitPackedLength(128, 4));
        assertEquals(80, ByteArrayInts.bitPackedLength(128, 5));
        assertEquals(128, ByteArrayInts.bitPackedLength(128, 8));
        assertEquals(208, ByteArrayInts.bitPackedLength(128, 13));
        // Partial trailing byte: 3 values at 5 bits = 15 bits = 2 bytes
        assertEquals(2, ByteArrayInts.bitPackedLength(3, 5));
    }

    /** Every value packed then unpacked matches; the trailing partial byte is zeroed correctly. */
    public void testBitPackedRoundTrip() {
        for (int bits : new int[] { 0, 1, 4, 5, 6, 7, 8, 9, 12, 13, 16, 17, 24, 31 }) {
            for (int count : new int[] { 0, 1, 3, 7, 8, 9, 127, 128, 129, 512 }) {
                final int max = bits == 0 ? 0 : bits == 31 ? Integer.MAX_VALUE : (1 << bits) - 1;
                final int[] src = new int[count];
                for (int i = 0; i < count; i++) {
                    src[i] = bits == 0 ? 0 : between(0, max);
                }
                final int len = ByteArrayInts.bitPackedLength(count, bits);
                final byte[] buf = new byte[len + 4]; // +4 to catch off-by-ones past the end
                final int offset = between(0, 4);
                final byte[] bufWithOffset = new byte[offset + len + 4];
                ByteArrayInts.writeBitPacked(src, count, bits, bufWithOffset, offset);
                final int[] dst = new int[count];
                ByteArrayInts.readBitPacked(bufWithOffset, offset, count, bits, dst);
                for (int i = 0; i < count; i++) {
                    assertEquals("bits=" + bits + " count=" + count + " at " + i, src[i], dst[i]);
                }
                // Bytes before and after the written region must be untouched
                for (int b = 0; b < offset; b++) {
                    assertEquals("bits=" + bits + " count=" + count + " byte before at " + b, 0, bufWithOffset[b]);
                }
            }
        }
    }

    /**
     * The bit-packing format matches what {@link org.elasticsearch.index.codec.tsdb.DocOffsetsCodec}
     * BITPACKING uses — MSB-first, same accumulator logic — so ColumNAR and TSDB bit-packed values
     * are encoded identically.
     */
    public void testBitPackedIsConsistentWithDocOffsetsCodecBitpacking() {
        // Three values at 5 bits: 10 (01010), 20 (10100), 5 (00101) → 01010 10100 00101 0 (padded)
        // Packed into bytes MSB-first: 0101 0101 = 0x55, 0000 1010 = 0x0A (with zero pad)
        // Byte layout: [0x55, 0x0A]
        final int[] values = { 10, 20, 5 };
        final byte[] buf = new byte[ByteArrayInts.bitPackedLength(3, 5)];
        ByteArrayInts.writeBitPacked(values, 3, 5, buf, 0);
        assertEquals("byte 0", (byte) 0b01010101, buf[0]);
        assertEquals("byte 1", (byte) 0b00001010, buf[1]);

        final int[] dst = new int[3];
        ByteArrayInts.readBitPacked(buf, 0, 3, 5, dst);
        for (int i = 0; i < values.length; i++) {
            assertEquals("at " + i, values[i], dst[i]);
        }
    }

    private static void assertVIntMatchesLucene(int value) throws IOException {
        final byte[] mine = new byte[ByteArrayInts.MAX_VINT_BYTES];
        final int written = ByteArrayInts.writeVInt(value, mine, 0);

        final byte[] lucene = new byte[ByteArrayInts.MAX_VINT_BYTES];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(lucene);
        out.writeVInt(value);

        assertEquals("byte count for " + value, out.getPosition(), written);
        assertEquals("bytes for " + value, new BytesRef(lucene, 0, out.getPosition()), new BytesRef(mine, 0, written));
        assertEquals("Lucene reads back " + value, value, new ByteArrayDataInput(mine, 0, written).readVInt());
        assertEquals("read back " + value, value, ByteArrayInts.readVInt(mine, 0));
    }

    private static void assertVLongMatchesLucene(long value) throws IOException {
        final byte[] mine = new byte[ByteArrayInts.MAX_VLONG_BYTES];
        final int written = ByteArrayInts.writeVLong(value, mine, 0);

        final byte[] lucene = new byte[ByteArrayInts.MAX_VLONG_BYTES];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(lucene);
        out.writeVLong(value);

        assertEquals("byte count for " + value, out.getPosition(), written);
        assertEquals("bytes for " + value, new BytesRef(lucene, 0, out.getPosition()), new BytesRef(mine, 0, written));
        assertEquals("Lucene reads back " + value, value, new ByteArrayDataInput(mine, 0, written).readVLong());
    }
}
