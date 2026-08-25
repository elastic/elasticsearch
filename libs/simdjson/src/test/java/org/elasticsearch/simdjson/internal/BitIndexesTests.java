/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.test.ESTestCase;

public class BitIndexesTests extends ESTestCase {

    // A single set bit produces one index at the correct position.
    public void testWriteAndReadSingleBit() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 1L << 5);
        bi.finish();
        assertEquals(5, bi.getAndAdvance());
    }

    // Multiple scattered bits produce the correct count and valid indices.
    public void testWriteMultipleBits() {
        BitIndexes bi = new BitIndexes(128);
        long bits = 0b1010101010101010L;
        int expectedCount = Long.bitCount(bits);
        bi.write(64, bits);
        bi.finish();
        assertEquals(expectedCount, bi.writeCount());
        for (int i = 0; i < expectedCount; i++) {
            assertFalse(bi.isEnd());
            int idx = bi.getAndAdvance();
            assertTrue(idx >= 0 && idx < 64);
        }
        assertTrue(bi.isEnd());
    }

    // 9-16 set bits exercise the second unrolled loop in write().
    public void testWrite9To16Bits() {
        BitIndexes bi = new BitIndexes(128);
        long bits = 0b111111111111L; // 12 bits set
        bi.write(64, bits);
        bi.finish();
        assertEquals(12, bi.writeCount());
        for (int i = 0; i < 12; i++) {
            assertEquals(i, bi.getAndAdvance());
        }
        assertTrue(bi.isEnd());
    }

    // All 64 bits set exercises all three unrolled loops in write().
    public void testWriteAllBitsSet() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0xFFFFFFFFFFFFFFFFL);
        bi.finish();
        assertEquals(64, bi.writeCount());
        for (int i = 0; i < 64; i++) {
            assertEquals(i, bi.getAndAdvance());
        }
        assertTrue(bi.isEnd());
    }

    // A zero bitmask writes nothing.
    public void testWriteZeroBitsIsNoop() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0L);
        assertEquals(0, bi.writeCount());
    }

    // Consecutive write() calls accumulate indices with correct block offsets.
    public void testMultipleWriteCallsAccumulate() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 1L << 3);
        bi.write(128, 1L << 7);
        bi.finish();
        assertEquals(2, bi.writeCount());
        assertEquals(3, bi.getAndAdvance());
        assertEquals(64 + 7, bi.getAndAdvance());
    }

    // reset() clears all state so the instance can be reused.
    public void testResetClearsState() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b111L);
        bi.reset();
        assertEquals(0, bi.writeCount());
        bi.finish();
        assertTrue(bi.isEnd());
    }

    // finish() writes a sentinel (0) at the writeIdx position.
    public void testFinishSetsSentinel() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 1L << 10);
        int writeCount = bi.writeCount();
        bi.finish();
        assertEquals(0, bi.getIndexAt(writeCount));
    }

    // advance() moves the cursor without returning a value.
    public void testAdvancePeekIsEnd() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b111L);
        bi.finish();
        assertEquals(0, bi.peek());
        assertEquals(0, bi.peek());
        bi.advance();
        assertEquals(1, bi.peek());
        bi.advance();
        bi.advance();
        assertTrue(bi.isEnd());
    }

    // advanceAndGet() moves the cursor forward and returns the value at the new position.
    public void testAdvanceAndGet() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b111L); // bits 0, 1, 2
        bi.finish();
        assertEquals(0, bi.peek());
        assertEquals(1, bi.advanceAndGet());
        assertEquals(2, bi.advanceAndGet());
        assertFalse(bi.isPastEnd());
        bi.advance();
        assertTrue(bi.isEnd());
    }

    // getLast() returns the final written index without moving the cursor.
    public void testGetLast() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b1101L); // bits 0, 2, 3
        bi.finish();
        assertEquals(3, bi.getLast());
        // cursor should be unaffected
        assertEquals(0, bi.peek());
    }

    // isPastEnd() is true only after reading beyond the last index.
    public void testIsPastEnd() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b1L);
        bi.finish();
        assertFalse(bi.isPastEnd());
        bi.getAndAdvance();
        assertFalse(bi.isPastEnd()); // at end, not past
        assertTrue(bi.isEnd());
        bi.advance();
        assertTrue(bi.isPastEnd());
    }

    // setReadWindow restricts iteration to a sub-range of the index array.
    public void testSetReadWindowSubRange() {
        BitIndexes bi = new BitIndexes(128);
        for (int i = 0; i < 10; i++) {
            bi.write(64, 1L << i);
        }
        bi.finish();
        bi.setReadWindow(3, 7);
        int count = 0;
        while (bi.hasNext()) {
            bi.advance();
            count++;
        }
        assertEquals(4, count);
    }

    // setReadWindow followed by getAndAdvance returns values from the windowed range.
    public void testSetReadWindowValues() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 10;
        raw[1] = 20;
        raw[2] = 30;
        raw[3] = 40;
        raw[4] = 50;
        bi.setWriteIdx(5);
        bi.finish();
        bi.setReadWindow(1, 4);
        assertEquals(20, bi.getAndAdvance());
        assertEquals(30, bi.getAndAdvance());
        assertEquals(40, bi.getAndAdvance());
        assertTrue(bi.isEnd());
    }

    // getIndexAt() reads by position without moving the cursor.
    public void testGetIndexAtRandomAccess() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b1111L);
        bi.finish();
        int peekBefore = bi.peek();
        int value = bi.getIndexAt(2);
        assertEquals(peekBefore, bi.peek());
        assertEquals(2, value);
    }

    // writeSentinel() overwrites a specific position with a chosen value.
    public void testWriteSentinel() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b11L);
        bi.finish();
        bi.writeSentinel(0, 99);
        assertEquals(99, bi.getIndexAt(0));
    }

    // findFirstIndexAtOrAfter() returns the position of the first index >= the target.
    public void testFindFirstIndexAtOrAfter() {
        BitIndexes bi = new BitIndexes(128);
        int[] offsets = { 10, 20, 30, 40 };
        int[] raw = bi.rawIndexes();
        for (int i = 0; i < offsets.length; i++) {
            raw[i] = offsets[i];
        }
        bi.setWriteIdx(offsets.length);
        bi.finish();
        int pos = bi.findFirstIndexAtOrAfter(0, 25);
        assertEquals(30, bi.getIndexAt(pos));
    }

    // findFirstIndexAtOrAfter() returns exact match position when target equals an index.
    public void testFindFirstIndexAtOrAfterExactMatch() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 10;
        raw[1] = 20;
        raw[2] = 30;
        raw[3] = 40;
        bi.setWriteIdx(4);
        bi.finish();
        int pos = bi.findFirstIndexAtOrAfter(0, 20);
        assertEquals(20, bi.getIndexAt(pos));
    }

    // findFirstIndexAtOrAfter() returns writeCount when target is beyond all indices.
    public void testFindFirstIndexAtOrAfterPastEnd() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 10;
        raw[1] = 20;
        raw[2] = 30;
        bi.setWriteIdx(3);
        bi.finish();
        int pos = bi.findFirstIndexAtOrAfter(0, 100);
        assertEquals(bi.writeCount(), pos);
    }

    // findFirstIndexAtOrAfter() with non-zero searchFrom skips earlier entries.
    public void testFindFirstIndexAtOrAfterWithSearchFrom() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 10;
        raw[1] = 20;
        raw[2] = 30;
        raw[3] = 40;
        bi.setWriteIdx(4);
        bi.finish();
        int pos = bi.findFirstIndexAtOrAfter(2, 15);
        assertEquals(2, pos);
        assertEquals(30, bi.getIndexAt(pos));
    }

    // ensureCapacity() grows the array and preserves existing data.
    public void testEnsureCapacityGrowsAndPreservesData() {
        BitIndexes bi = new BitIndexes(4);
        int[] raw = bi.rawIndexes();
        raw[0] = 42;
        raw[1] = 99;
        bi.setWriteIdx(2);

        bi.ensureCapacity(10);
        assertTrue(bi.rawIndexes().length >= 10);
        assertNotSame(raw, bi.rawIndexes());
        assertEquals(42, bi.getIndexAt(0));
        assertEquals(99, bi.getIndexAt(1));
        assertEquals(2, bi.writeCount());
    }

    // ensureCapacity() is a no-op when current capacity is sufficient.
    public void testEnsureCapacityNoOp() {
        BitIndexes bi = new BitIndexes(128);
        int[] before = bi.rawIndexes();
        bi.ensureCapacity(64);
        assertSame(before, bi.rawIndexes());
    }

    // Exactly 8 bits exercises only the first unrolled loop in write().
    public void testWriteExactly8Bits() {
        BitIndexes bi = new BitIndexes(128);
        long bits = 0xFFL; // bits 0-7
        bi.write(64, bits);
        bi.finish();
        assertEquals(8, bi.writeCount());
        for (int i = 0; i < 8; i++) {
            assertEquals(i, bi.getAndAdvance());
        }
        assertTrue(bi.isEnd());
    }

    // Exactly 16 bits exercises the first and second unrolled loops but not the do-while tail.
    public void testWriteExactly16Bits() {
        BitIndexes bi = new BitIndexes(128);
        long bits = 0xFFFFL; // bits 0-15
        bi.write(64, bits);
        bi.finish();
        assertEquals(16, bi.writeCount());
        for (int i = 0; i < 16; i++) {
            assertEquals(i, bi.getAndAdvance());
        }
        assertTrue(bi.isEnd());
    }

    // Multiple bits in a non-zero block verify blockIndex offset arithmetic.
    public void testWriteMultipleBitsWithBlockOffset() {
        BitIndexes bi = new BitIndexes(128);
        long bits = 0b1011L; // bits 0, 1, 3
        bi.write(192, bits);
        bi.finish();
        assertEquals(3, bi.writeCount());
        assertEquals(128, bi.getAndAdvance());
        assertEquals(129, bi.getAndAdvance());
        assertEquals(131, bi.getAndAdvance());
    }

    // ensureCapacity() grows to at least 2x when 2x exceeds the requested minimum.
    public void testEnsureCapacityGeometricGrowth() {
        BitIndexes bi = new BitIndexes(8);
        bi.ensureCapacity(10);
        assertTrue(bi.rawIndexes().length >= 16);
    }

    // findFirstIndexAtOrAfter() matches the very first element.
    public void testFindFirstIndexAtOrAfterFirstElement() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 5;
        raw[1] = 15;
        raw[2] = 25;
        bi.setWriteIdx(3);
        bi.finish();
        int pos = bi.findFirstIndexAtOrAfter(0, 5);
        assertEquals(0, pos);
        assertEquals(5, bi.getIndexAt(pos));
    }

    // reset() followed by new write() produces fresh indices starting from position 0.
    public void testResetThenWrite() {
        BitIndexes bi = new BitIndexes(128);
        bi.write(64, 0b111L);
        bi.reset();
        bi.write(64, 1L << 10);
        bi.finish();
        assertEquals(1, bi.writeCount());
        assertEquals(10, bi.getAndAdvance());
        assertTrue(bi.isEnd());
    }

    // hasNext() returns false on an empty BitIndexes with no writes.
    public void testHasNextOnEmpty() {
        BitIndexes bi = new BitIndexes(128);
        bi.finish();
        assertFalse(bi.hasNext());
    }

    // rawIndexes()/setWriteIdx() allow bulk-writing indices and reading them back.
    public void testRawIndexesAndSetWriteIdx() {
        BitIndexes bi = new BitIndexes(128);
        int[] raw = bi.rawIndexes();
        raw[0] = 5;
        raw[1] = 15;
        raw[2] = 25;
        bi.setWriteIdx(3);
        bi.finish();
        assertEquals(3, bi.writeCount());
        assertEquals(5, bi.getAndAdvance());
        assertEquals(15, bi.getAndAdvance());
        assertEquals(25, bi.getAndAdvance());
    }
}
