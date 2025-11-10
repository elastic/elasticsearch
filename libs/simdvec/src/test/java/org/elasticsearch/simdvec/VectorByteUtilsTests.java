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

@com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
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

    public void testEqualMaskWithRandomMatches() {
        // Generate random array
        int length = randomInt(1025);
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
