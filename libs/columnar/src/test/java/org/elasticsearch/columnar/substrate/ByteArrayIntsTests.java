/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

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
    public void testVIntAtAnOffset() {
        final byte[] buffer = new byte[64];
        final int offset = between(1, 32);
        final int value = randomNonNegativeInt();
        final int written = ByteArrayInts.writeVInt(value, buffer, offset);
        assertEquals("bytes written", ByteArrayInts.vIntLength(value), written);
        assertEquals("value", value, ByteArrayInts.readVInt(buffer, offset));
        assertEquals("nothing written before the offset", 0, buffer[offset - 1]);
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
