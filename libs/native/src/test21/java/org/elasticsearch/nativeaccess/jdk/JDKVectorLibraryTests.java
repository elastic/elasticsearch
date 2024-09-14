/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryTests extends VectorSimilarityFunctionsTests {

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    static final int[] VECTOR_DIMS = { 1, 4, 6, 8, 13, 16, 25, 31, 32, 33, 64, 100, 128, 207, 256, 300, 512, 702, 1023, 1024, 1025 };

    final int size;

    static Arena arena;

    public JDKVectorLibraryTests(int size) {
        this.size = size;
    }

    @BeforeClass
    public static void setup() {
        arena = Arena.ofConfined();
    }

    @AfterClass
    public static void cleanup() {
        arena.close();
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> IntStream.of(VECTOR_DIMS).boxed().map(i -> new Object[] { i }).iterator();
    }

    public void testInt7BinaryVectors() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(values[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(MemorySegment.ofArray(values[i]), 0L, segment, (long) i * dims, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims, dims);
            var nativeSeg2 = segment.asSlice((long) second * dims, dims);

            // dot product
            int expected = dotProductScalar(values[first], values[second]);
            assertEquals(expected, dotProduct7u(nativeSeg1, nativeSeg2, dims));
            if (testWithHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, dotProduct7u(heapSeg1, heapSeg2, dims));
                assertEquals(expected, dotProduct7u(nativeSeg1, heapSeg2, dims));
                assertEquals(expected, dotProduct7u(heapSeg1, nativeSeg2, dims));
            }

            // square distance
            expected = squareDistanceScalar(values[first], values[second]);
            assertEquals(expected, squareDistance7u(nativeSeg1, nativeSeg2, dims));
            if (testWithHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, squareDistance7u(heapSeg1, heapSeg2, dims));
                assertEquals(expected, squareDistance7u(nativeSeg1, heapSeg2, dims));
                assertEquals(expected, squareDistance7u(heapSeg1, nativeSeg2, dims));
            }
        }
    }

    static boolean testWithHeapSegments() {
        return Runtime.version().feature() >= 22;
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3);
        var e = expectThrows(IAE, () -> dotProduct7u(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e.getMessage(), containsString("dimensions differ"));

        e = expectThrows(IAE, () -> dotProduct7u(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e.getMessage(), containsString("greater than vector dimensions"));
    }

    int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) getVectorDistance().dotProductHandle7u().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) getVectorDistance().squareDistanceHandle7u().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /** Computes the dot product of the given vectors a and b. */
    static int dotProductScalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    /** Computes the square distance of the given vectors a and b. */
    static int squareDistanceScalar(byte[] a, byte[] b) {
        // Note: this will not overflow if dim < 2^18, since max(byte * byte) = 2^14.
        int squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }
}
