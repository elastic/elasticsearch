/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.simdvec.internal.vectorization.BaseVectorizationTests;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
public class VectorByteUtilsTests extends BaseVectorizationTests {

    static final VectorByteUtils defaultVectorByteUtils = defaultProvider().getVectorUtilSupport().getVectorByteUtils();
    static final VectorByteUtils panamaVectorByteUtils = maybePanamaProvider().getVectorUtilSupport().getVectorByteUtils();

    public void testEqualMaskNoMatches() {
        byte[] data = new byte[randomInt(1025)];

        // Fill array with random bytes != 1
        for (int i = 0; i < data.length; i++) {
            data[i] = randomByteOtherThan((byte) 1);
        }

        for (int i = 0; i < data.length; i += defaultVectorByteUtils.vectorLength()) {
            long mask = defaultVectorByteUtils.equalMask(data, i, (byte) 1);
            assertFalse("No bits should be set", VectorByteUtils.anyTrue(mask));
            assertEquals(-1, VectorByteUtils.firstSet(mask));
            assertEquals(-1, VectorByteUtils.nextSet(mask, 0));
        }

        final int len = panamaVectorByteUtils.vectorLength();
        assertTrue("Vector length must be power of two", isPowerOfTwo(len));
        final int bound = VectorByteUtils.loopBound(data.length, len);
        int i = 0;
        for (; i < bound; i += len) {
            long mask = panamaVectorByteUtils.equalMask(data, i, (byte) 1);
            assertFalse("No bits should be set", VectorByteUtils.anyTrue(mask));
            assertEquals(-1, VectorByteUtils.firstSet(mask));
            assertEquals(-1, VectorByteUtils.nextSet(mask, 0));
        }
        // scalar tail
        for (; i < data.length; i++) {
            assertTrue(data[i] != (byte) 1);
        }
    }

    // Always test with a few known boundaries sizes.
    public void testEqualMaskWithRandomMatchesKnownSizes() {
        testEqualMaskWithRandomMatches(0);
        testEqualMaskWithRandomMatches(1);
        testEqualMaskWithRandomMatches(63);
        testEqualMaskWithRandomMatches(64);
        testEqualMaskWithRandomMatches(65);
        testEqualMaskWithRandomMatches(127);
        testEqualMaskWithRandomMatches(128);
        testEqualMaskWithRandomMatches(129);
        testEqualMaskWithRandomMatches(255);
        testEqualMaskWithRandomMatches(256);
        testEqualMaskWithRandomMatches(257);
        testEqualMaskWithRandomMatches(511);
        testEqualMaskWithRandomMatches(512);
        testEqualMaskWithRandomMatches(513);
        testEqualMaskWithRandomMatches(1023);
        testEqualMaskWithRandomMatches(1024);
        testEqualMaskWithRandomMatches(1025);
    }

    public void testEqualMaskWithRandomMatches() {
        testEqualMaskWithRandomMatches(randomInt(10250));
    }

    private void testEqualMaskWithRandomMatches(int length) {
        byte[] data = new byte[length];
        // Fill array with random bytes in range 0..7
        for (int i = 0; i < length; i++) {
            data[i] = (byte) random().nextInt(8);
        }
        byte target = (byte) random().nextInt(8);

        // Track expected match positions
        boolean[] expected = new boolean[length];
        for (int i = 0; i < length; i++) {
            expected[i] = data[i] == target;
        }

        // Test defaultVectorByteUtils
        for (int i = 0; i < length; i += defaultVectorByteUtils.vectorLength()) {
            long mask = defaultVectorByteUtils.equalMask(data, i, target);

            int pos = VectorByteUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                assertTrue("Match should be correct", expected[idx]);
                pos = VectorByteUtils.nextSet(mask, pos);
            }
        }

        // Test panamaVectorByteUtils
        final int len = panamaVectorByteUtils.vectorLength();
        assertTrue("Vector length must be power of two", isPowerOfTwo(len));
        final int bound = VectorByteUtils.loopBound(length, len);

        int i = 0;
        for (; i < bound; i += len) {
            long mask = panamaVectorByteUtils.equalMask(data, i, target);
            int pos = VectorByteUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                assertTrue("Match should be correct", expected[idx]);
                pos = VectorByteUtils.nextSet(mask, pos);
            }
        }
        // scalar tail
        for (; i < data.length; i++) {
            if (data[i] == target) {
                assertTrue("Scalar tail match should exist", expected[i]);
            } else {
                assertFalse("Scalar tail non-match", expected[i]);
            }
        }
    }

    public void testNextSetBeyondLastBit() {
        long mask = 0b10100000L;
        assertEquals(-1, VectorByteUtils.nextSet(mask, 7)); // beyond all bits
        assertEquals(5, VectorByteUtils.firstSet(mask));
        assertEquals(7, VectorByteUtils.nextSet(mask, 5));
        assertEquals(-1, VectorByteUtils.nextSet(mask, 7));
    }

    public void testNextSetFromIndex63() {
        // Only the top bit (bit 63) is set
        long mask = 1L << 63;
        // fromIndex == 62, should find bit 63
        assertEquals(63, VectorByteUtils.nextSet(mask, 62));
        // fromIndex == 63, should safely return -1 (no bits beyond 63)
        assertEquals(-1, VectorByteUtils.nextSet(mask, 63));
    }

    public void testNextSetEmptyMask() {
        long mask = 0L;
        assertEquals(-1, VectorByteUtils.nextSet(mask, -1));
        assertEquals(-1, VectorByteUtils.nextSet(mask, 0));
        assertEquals(-1, VectorByteUtils.nextSet(mask, 63));
    }

    public void testNextSetSingleBitAtVariousPositions() {
        for (int pos = 0; pos < Long.SIZE; pos++) {
            long mask = 1L << pos;
            // fromIndex less than pos, should find pos
            assertEquals(pos, VectorByteUtils.nextSet(mask, pos - 1));
            // fromIndex == pos, should find nothing after it
            assertEquals(-1, VectorByteUtils.nextSet(mask, pos));
        }
    }

    public void testSequentialIteration() {
        long mask = 0b101101L; // bits at 0, 2, 3, 5
        int pos = VectorByteUtils.firstSet(mask);
        assertEquals(0, VectorByteUtils.firstSet(mask));
        pos = VectorByteUtils.nextSet(mask, pos);
        assertEquals(2, pos);
        pos = VectorByteUtils.nextSet(mask, pos);
        assertEquals(3, pos);
        pos = VectorByteUtils.nextSet(mask, pos);
        assertEquals(5, pos);
        pos = VectorByteUtils.nextSet(mask, pos);
        assertEquals(-1, pos);
    }

    public void testFirstSetEmptyMask() {
        long mask = 0L;
        assertEquals("Empty mask should return -1", -1, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitAtLSB() {
        long mask = 1L; // bit 0 set
        assertEquals("First set bit at position 0", 0, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitAtMSB() {
        long mask = 1L << 63; // bit 63 set
        assertEquals("First set bit at position 63", 63, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetConsecutiveBits() {
        long mask = 0b11100L; // bits at 2, 3, 4
        assertEquals("Lowest set bit is 2", 2, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetRandomBits() {
        long mask = (1L << 10) | (1L << 30) | (1L << 45);
        assertEquals("Lowest set bit is 10", 10, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetAllBitsSet() {
        long mask = ~0L; // all 64 bits set
        assertEquals("Lowest set bit is 0", 0, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetHighOnlyBits() {
        long mask = 0xF000000000000000L; // bits 60–63 set
        assertEquals("Lowest set bit among high bits is 60", 60, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetAlternatingBits() {
        long mask = 0xAAAAAAAAAAAAAAAAL; // alternating 1010...
        assertEquals("Lowest set bit is at position 1", 1, VectorByteUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitEachPosition() {
        for (int i = 0; i < Long.SIZE; i++) {
            long mask = 1L << i;
            assertEquals("First set bit should match position", i, VectorByteUtils.firstSet(mask));
        }
    }

    public void testLoopBoundExactMultiple() {
        // length is already a multiple of laneCount
        assertEquals(64, VectorByteUtils.loopBound(64, 8));
        assertEquals(128, VectorByteUtils.loopBound(128, 16));
        assertEquals(128, VectorByteUtils.loopBound(128, 32));
        assertEquals(256, VectorByteUtils.loopBound(256, 64));
    }

    public void testLoopBoundNonMultiple() {
        // length is not a multiple, round down to the nearest multiple
        assertEquals(56, VectorByteUtils.loopBound(63, 8));
        assertEquals(48, VectorByteUtils.loopBound(55, 16));
        assertEquals(32, VectorByteUtils.loopBound(55, 32));
        assertEquals(192, VectorByteUtils.loopBound(255, 64));
    }

    public void testLoopBoundSmallerThanLaneCount() {
        // length smaller than laneCount, result is 0
        assertEquals(0, VectorByteUtils.loopBound(3, 8));
        assertEquals(0, VectorByteUtils.loopBound(15, 16));
        assertEquals(0, VectorByteUtils.loopBound(29, 32));
        assertEquals(0, VectorByteUtils.loopBound(63, 64));
    }

    public void testLoopBoundLengthZero() {
        assertEquals(0, VectorByteUtils.loopBound(0, 8));
        assertEquals(0, VectorByteUtils.loopBound(0, 16));
        assertEquals(0, VectorByteUtils.loopBound(0, 32));
        assertEquals(0, VectorByteUtils.loopBound(0, 64));
    }

    public void testLoopBoundLaneCountOne() {
        // laneCount = 1, loopBound == length
        for (int len = 0; len < 10; len++) {
            assertEquals(len, VectorByteUtils.loopBound(len, 1));
        }
    }

    public void testLoopBoundPowersOfTwo() {
        // verify behavior for a variety of power-of-two lane counts
        int[] laneCounts = { 2, 4, 8, 16, 32, 64 };
        int length = 123;
        for (int laneCount : laneCounts) {
            int bound = VectorByteUtils.loopBound(length, laneCount);
            assertEquals("Bound should be a multiple of laneCount", 0, bound % laneCount);
            assertTrue("Bound should be <= length", bound <= length);
        }
    }

    public void testLoopBoundInvalidLaneCountZero() {
        expectThrows(AssertionError.class, () -> VectorByteUtils.loopBound(10, 0));
    }

    public void testLoopBoundInvalidLaneCountNonPowerOfTwo() {
        expectThrows(AssertionError.class, () -> VectorByteUtils.loopBound(10, 3)); // not a power of two
    }

    static boolean isPowerOfTwo(int n) {
        return n > 0 && (n & (n - 1)) == 0;
    }

    static byte randomByteOtherThan(byte value) {
        byte b;
        do {
            b = (byte) random().nextInt(256);
        } while (b == value);
        return b;
    }
}
