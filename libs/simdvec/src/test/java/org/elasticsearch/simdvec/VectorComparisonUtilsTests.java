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

import java.util.stream.IntStream;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
public class VectorComparisonUtilsTests extends BaseVectorizationTests {

    static final VectorComparisonUtils defaultVectorCmpUtils = defaultProvider().getVectorUtilSupport().getVectorComparisonUtils();
    static final VectorComparisonUtils panamaVectorCmpUtils = maybePanamaProvider().getVectorUtilSupport().getVectorComparisonUtils();

    public void testLanesMustBePowerOfTwo() {
        assertTrue("Vector length must be power of two", isPowerOfTwo(defaultVectorCmpUtils.byteVectorLanes()));
        assertTrue("Vector length must be power of two", isPowerOfTwo(panamaVectorCmpUtils.byteVectorLanes()));
    }

    // -- bytes

    public void testEqualMaskNoMatchesByte() {
        // Use a target value of 1, and fill the array with random bytes other than 1
        byte[] data = new byte[randomInt(1025)];
        IntStream.range(0, data.length).forEach(i -> data[i] = randomByteOtherThan((byte) 1));
        boolean[] expected = new boolean[data.length]; // all false - no matches
        assertVCEqual(expected, data, (byte) 1, defaultVectorCmpUtils);
        assertVCEqual(expected, data, (byte) 1, panamaVectorCmpUtils);
    }

    // Always test with a few known boundaries sizes.
    public void testEqualMaskWithRandomMatchesKnownSizesByte() {
        testEqualMaskWithRandomMatchesByte(0);
        testEqualMaskWithRandomMatchesByte(1);
        testEqualMaskWithRandomMatchesByte(63);
        testEqualMaskWithRandomMatchesByte(64);
        testEqualMaskWithRandomMatchesByte(65);
        testEqualMaskWithRandomMatchesByte(127);
        testEqualMaskWithRandomMatchesByte(128);
        testEqualMaskWithRandomMatchesByte(129);
        testEqualMaskWithRandomMatchesByte(255);
        testEqualMaskWithRandomMatchesByte(256);
        testEqualMaskWithRandomMatchesByte(257);
        testEqualMaskWithRandomMatchesByte(511);
        testEqualMaskWithRandomMatchesByte(512);
        testEqualMaskWithRandomMatchesByte(513);
        testEqualMaskWithRandomMatchesByte(1023);
        testEqualMaskWithRandomMatchesByte(1024);
        testEqualMaskWithRandomMatchesByte(1025);
    }

    public void testEqualMaskWithRandomMatchesByte() {
        testEqualMaskWithRandomMatchesByte(randomInt(10250));
    }

    void testEqualMaskWithRandomMatchesByte(int length) {
        byte[] data = new byte[length];
        // Fill array with random bytes in range 0..7
        IntStream.range(0, length).forEach(i -> data[i] = (byte) random().nextInt(8));
        byte target = (byte) random().nextInt(8);

        // Track expected match positions
        boolean[] expected = new boolean[length];
        IntStream.range(0, length).forEach(i -> expected[i] = data[i] == target);

        assertVCEqual(expected, data, target, defaultVectorCmpUtils);
        assertVCEqual(expected, data, target, panamaVectorCmpUtils);
    }

    void assertVCEqual(boolean[] expected, byte[] data, byte target, VectorComparisonUtils comparisonUtils) {
        final int length = expected.length;
        final int len = comparisonUtils.byteVectorLanes();
        final int bound = VectorComparisonUtils.loopBound(length, len);

        int i = 0;
        for (; i < bound; i += len) {
            long mask = comparisonUtils.equalMask(data, i, target);
            int pos = VectorComparisonUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                assertTrue("Match should be correct", expected[idx]);
                pos = VectorComparisonUtils.nextSet(mask, pos);
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

    public void testEqualMaskUnalignedByte() {
        byte[] data = new byte[256];
        data[2] = 1;
        assertEquals(0b100, panamaVectorCmpUtils.equalMask(data, 0, (byte) 1));
        assertEquals(0b010, panamaVectorCmpUtils.equalMask(data, 1, (byte) 1));
        assertEquals(0b001, panamaVectorCmpUtils.equalMask(data, 2, (byte) 1));
    }

    // -- long variants

    public void testEqualMaskNoMatchesLong() {
        // Use a target value of 1, and fill the array with random bytes other than 1
        long[] data = new long[randomInt(1025)];
        IntStream.range(0, data.length).forEach(i -> data[i] = randomLongOtherThan((byte) 1));
        boolean[] expected = new boolean[data.length]; // all false - no matches
        assertVCEqual(expected, data, (byte) 1, defaultVectorCmpUtils);
        assertVCEqual(expected, data, (byte) 1, panamaVectorCmpUtils);
    }

    // Always test with a few known boundaries sizes.
    public void testEqualMaskWithRandomMatchesKnownSizesLong() {
        testEqualMaskWithRandomMatchesLong(0);
        testEqualMaskWithRandomMatchesLong(1);
        testEqualMaskWithRandomMatchesLong(63);
        testEqualMaskWithRandomMatchesLong(64);
        testEqualMaskWithRandomMatchesLong(65);
        testEqualMaskWithRandomMatchesLong(127);
        testEqualMaskWithRandomMatchesLong(128);
        testEqualMaskWithRandomMatchesLong(129);
        testEqualMaskWithRandomMatchesLong(255);
        testEqualMaskWithRandomMatchesLong(256);
        testEqualMaskWithRandomMatchesLong(257);
        testEqualMaskWithRandomMatchesLong(511);
        testEqualMaskWithRandomMatchesLong(512);
        testEqualMaskWithRandomMatchesLong(513);
        testEqualMaskWithRandomMatchesLong(1023);
        testEqualMaskWithRandomMatchesLong(1024);
        testEqualMaskWithRandomMatchesLong(1025);
    }

    public void testEqualMaskWithRandomMatchesLong() {
        testEqualMaskWithRandomMatchesLong(randomInt(10250));
    }

    void testEqualMaskWithRandomMatchesLong(int length) {
        long[] data = new long[length];
        // Fill array with random bytes in range 0..7
        IntStream.range(0, length).forEach(i -> data[i] = random().nextLong(8));
        long target = random().nextLong(8);

        // Track expected match positions
        boolean[] expected = new boolean[length];
        IntStream.range(0, length).forEach(i -> expected[i] = data[i] == target);

        assertVCEqual(expected, data, target, defaultVectorCmpUtils);
        assertVCEqual(expected, data, target, panamaVectorCmpUtils);
    }

    void assertVCEqual(boolean[] expected, long[] data, long target, VectorComparisonUtils comparisonUtils) {
        final int length = expected.length;
        final int len = comparisonUtils.longVectorLanes();
        final int bound = VectorComparisonUtils.loopBound(length, len);

        int i = 0;
        for (; i < bound; i += len) {
            long mask = comparisonUtils.equalMask(data, i, target);
            int pos = VectorComparisonUtils.firstSet(mask);
            while (pos >= 0) {
                int idx = i + pos;
                assertTrue("Match should be correct", expected[idx]);
                pos = VectorComparisonUtils.nextSet(mask, pos);
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

    public void testEqualMaskUnalignedLong() {
        long[] data = new long[16];
        data[1] = 1L;
        if (panamaVectorCmpUtils.longVectorLanes() == 1) {
            assertEquals(0b0, panamaVectorCmpUtils.equalMask(data, 0, 1L));
            assertEquals(0b1, panamaVectorCmpUtils.equalMask(data, 1, 1L));
        } else {
            assertEquals(0b10, panamaVectorCmpUtils.equalMask(data, 0, 1L));
            assertEquals(0b01, panamaVectorCmpUtils.equalMask(data, 1, 1L));
        }
    }

    // -- static utilities

    public void testAnyTrueMask() {
        assertFalse(VectorComparisonUtils.anyTrue(0L));
        assertTrue(VectorComparisonUtils.anyTrue(1L));
        assertTrue(VectorComparisonUtils.anyTrue(-1L));
        assertTrue(VectorComparisonUtils.anyTrue(Long.MAX_VALUE));
    }

    public void testNextSetBeyondLastBit() {
        long mask = 0b10100000L;
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, 7)); // beyond all bits
        assertEquals(5, VectorComparisonUtils.firstSet(mask));
        assertEquals(7, VectorComparisonUtils.nextSet(mask, 5));
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, 7));
    }

    public void testNextSetFromIndex63() {
        // Only the top bit (bit 63) is set
        long mask = 1L << 63;
        // fromIndex == 62, should find bit 63
        assertEquals(63, VectorComparisonUtils.nextSet(mask, 62));
        // fromIndex == 63, should safely return -1 (no bits beyond 63)
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, 63));
    }

    public void testNextSetEmptyMask() {
        long mask = 0L;
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, -1));
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, 0));
        assertEquals(-1, VectorComparisonUtils.nextSet(mask, 63));
    }

    public void testNextSetSingleBitAtVariousPositions() {
        for (int pos = 0; pos < Long.SIZE; pos++) {
            long mask = 1L << pos;
            // fromIndex less than pos, should find pos
            assertEquals(pos, VectorComparisonUtils.nextSet(mask, pos - 1));
            // fromIndex == pos, should find nothing after it
            assertEquals(-1, VectorComparisonUtils.nextSet(mask, pos));
        }
    }

    public void testSequentialIteration() {
        long mask = 0b101101L; // bits at 0, 2, 3, 5
        int pos = VectorComparisonUtils.firstSet(mask);
        assertEquals(0, VectorComparisonUtils.firstSet(mask));
        pos = VectorComparisonUtils.nextSet(mask, pos);
        assertEquals(2, pos);
        pos = VectorComparisonUtils.nextSet(mask, pos);
        assertEquals(3, pos);
        pos = VectorComparisonUtils.nextSet(mask, pos);
        assertEquals(5, pos);
        pos = VectorComparisonUtils.nextSet(mask, pos);
        assertEquals(-1, pos);
    }

    public void testFirstSetEmptyMask() {
        long mask = 0L;
        assertEquals("Empty mask should return -1", -1, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitAtLSB() {
        long mask = 1L; // bit 0 set
        assertEquals("First set bit at position 0", 0, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitAtMSB() {
        long mask = 1L << 63; // bit 63 set
        assertEquals("First set bit at position 63", 63, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetConsecutiveBits() {
        long mask = 0b11100L; // bits at 2, 3, 4
        assertEquals("Lowest set bit is 2", 2, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetRandomBits() {
        long mask = (1L << 10) | (1L << 30) | (1L << 45);
        assertEquals("Lowest set bit is 10", 10, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetAllBitsSet() {
        long mask = ~0L; // all 64 bits set
        assertEquals("Lowest set bit is 0", 0, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetHighOnlyBits() {
        long mask = 0xF000000000000000L; // bits 60–63 set
        assertEquals("Lowest set bit among high bits is 60", 60, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetAlternatingBits() {
        long mask = 0xAAAAAAAAAAAAAAAAL; // alternating 1010...
        assertEquals("Lowest set bit is at position 1", 1, VectorComparisonUtils.firstSet(mask));
    }

    public void testFirstSetSingleBitEachPosition() {
        for (int i = 0; i < Long.SIZE; i++) {
            long mask = 1L << i;
            assertEquals("First set bit should match position", i, VectorComparisonUtils.firstSet(mask));
        }
    }

    public void testLoopBoundExactMultiple() {
        // length is already a multiple of laneCount
        assertEquals(64, VectorComparisonUtils.loopBound(64, 8));
        assertEquals(128, VectorComparisonUtils.loopBound(128, 16));
        assertEquals(128, VectorComparisonUtils.loopBound(128, 32));
        assertEquals(256, VectorComparisonUtils.loopBound(256, 64));
    }

    public void testLoopBoundNonMultiple() {
        // length is not a multiple, round down to the nearest multiple
        assertEquals(56, VectorComparisonUtils.loopBound(63, 8));
        assertEquals(48, VectorComparisonUtils.loopBound(55, 16));
        assertEquals(32, VectorComparisonUtils.loopBound(55, 32));
        assertEquals(192, VectorComparisonUtils.loopBound(255, 64));
    }

    public void testLoopBoundSmallerThanLaneCount() {
        // length smaller than laneCount, result is 0
        assertEquals(0, VectorComparisonUtils.loopBound(3, 8));
        assertEquals(0, VectorComparisonUtils.loopBound(15, 16));
        assertEquals(0, VectorComparisonUtils.loopBound(29, 32));
        assertEquals(0, VectorComparisonUtils.loopBound(63, 64));
    }

    public void testLoopBoundLengthZero() {
        assertEquals(0, VectorComparisonUtils.loopBound(0, 8));
        assertEquals(0, VectorComparisonUtils.loopBound(0, 16));
        assertEquals(0, VectorComparisonUtils.loopBound(0, 32));
        assertEquals(0, VectorComparisonUtils.loopBound(0, 64));
    }

    public void testLoopBoundLaneCountOne() {
        // laneCount = 1, loopBound == length
        for (int len = 0; len < 10; len++) {
            assertEquals(len, VectorComparisonUtils.loopBound(len, 1));
        }
    }

    public void testLoopBoundPowersOfTwo() {
        // verify behavior for a variety of power-of-two lane counts
        int[] laneCounts = { 2, 4, 8, 16, 32, 64 };
        int length = 123;
        for (int laneCount : laneCounts) {
            int bound = VectorComparisonUtils.loopBound(length, laneCount);
            assertEquals("Bound should be a multiple of laneCount", 0, bound % laneCount);
            assertTrue("Bound should be <= length", bound <= length);
        }
    }

    public void testLoopBoundInvalidLaneCountZero() {
        expectThrows(AssertionError.class, () -> VectorComparisonUtils.loopBound(10, 0));
    }

    public void testLoopBoundInvalidLaneCountNonPowerOfTwo() {
        expectThrows(AssertionError.class, () -> VectorComparisonUtils.loopBound(10, 3)); // not a power of two
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

    static long randomLongOtherThan(long value) {
        long l;
        do {
            l = random().nextLong();
        } while (l == value);
        return l;
    }
}
