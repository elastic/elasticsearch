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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryInt7uTests extends VectorSimilarityFunctionsTests {

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    public JDKVectorLibraryInt7uTests(int size) {
        super(size);
    }

    @BeforeClass
    public static void beforeClass() {
        VectorSimilarityFunctionsTests.setup();
    }

    @AfterClass
    public static void afterClass() {
        VectorSimilarityFunctionsTests.cleanup();
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return VectorSimilarityFunctionsTests.parametersFactory();
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
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, dotProduct7u(heapSeg1, heapSeg2, dims));
                assertEquals(expected, dotProduct7u(nativeSeg1, heapSeg2, dims));
                assertEquals(expected, dotProduct7u(heapSeg1, nativeSeg2, dims));

                // trivial bulk with a single vector
                float[] bulkScore = new float[1];
                dotProduct7uBulk(nativeSeg1, nativeSeg2, dims, 1, MemorySegment.ofArray(bulkScore));
                assertEquals(expected, bulkScore[0], 0f);
            }

            // square distance
            expected = squareDistanceScalar(values[first], values[second]);
            assertEquals(expected, squareDistance7u(nativeSeg1, nativeSeg2, dims));
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, squareDistance7u(heapSeg1, heapSeg2, dims));
                assertEquals(expected, squareDistance7u(nativeSeg1, heapSeg2, dims));
                assertEquals(expected, squareDistance7u(heapSeg1, nativeSeg2, dims));
            }
        }
    }

    public void testInt7uBulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(values[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(MemorySegment.ofArray(values[i]), 0L, segment, (long) i * dims, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        dotProductBulkScalar(values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        dotProduct7uBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            dotProduct7uBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, 0f);
        }
    }

    public void testInt7uBulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new byte[numVecs][dims];
        var vectorsSegment = arena.allocate((long) dims * numVecs);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(vectors[i], 0, vectorsSegment, ValueLayout.JAVA_BYTE, (long) i * dims, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        dotProductBulkWithOffsetsScalar(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        dotProduct7uBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, dims, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt7uBulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new byte[numVecs][dims];

        // Mimics extra data at the end
        var pitch = dims * Byte.BYTES + Float.BYTES;
        var vectorsSegment = arena.allocate((long) numVecs * pitch);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(vectors[i], 0, vectorsSegment, ValueLayout.JAVA_BYTE, (long) i * pitch, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        dotProductBulkWithOffsetsScalar(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * pitch, pitch);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        dotProduct7uBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt7uBulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            randomBytesBetween(values[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(MemorySegment.ofArray(values[i]), 0L, segment, (long) i * dims, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        dotProductBulkWithOffsetsScalar(values[queryOrd], values, offsets, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);

        float[] bulkScores = new float[numVecs];
        dotProduct7uBulkWithOffsets(
            segment,
            nativeQuerySeg,
            dims,
            dims,
            MemorySegment.ofArray(offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, 0f);
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3);

        var e1 = expectThrows(IAE, () -> dotProduct7u(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e1.getMessage(), containsString("dimensions differ"));

        var e2 = expectThrows(IOOBE, () -> dotProduct7u(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));

        var e3 = expectThrows(IOOBE, () -> dotProduct7u(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(e3.getMessage(), containsString("out of bounds for length"));

        var e4 = expectThrows(IAE, () -> squareDistance7u(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e4.getMessage(), containsString("dimensions differ"));

        var e5 = expectThrows(IOOBE, () -> squareDistance7u(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e5.getMessage(), containsString("out of bounds for length"));

        var e6 = expectThrows(IOOBE, () -> squareDistance7u(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(e6.getMessage(), containsString("out of bounds for length"));
    }

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segA = arena.allocate((long) size * 3);
        var segB = arena.allocate((long) size * 3);
        var segS = arena.allocate((long) size * Float.BYTES);

        var e1 = expectThrows(IOOBE, () -> dotProduct7uBulk(segA, segB, size, 4, segS));
        assertThat(e1.getMessage(), containsString("out of bounds for length"));

        var e2 = expectThrows(IOOBE, () -> dotProduct7uBulk(segA, segB, size, -1, segS));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));

        var e3 = expectThrows(IOOBE, () -> dotProduct7uBulk(segA, segB, -1, 3, segS));
        assertThat(e3.getMessage(), containsString("out of bounds for length"));

        var tooSmall = arena.allocate((long) 3 * Float.BYTES - 1);
        var e4 = expectThrows(IOOBE, () -> dotProduct7uBulk(segA, segB, size, 3, tooSmall));
        assertThat(e4.getMessage(), containsString("out of bounds for length"));
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

    void dotProduct7uBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().dotProductHandle7uBulk().invokeExact(a, b, dims, count, result);
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

    void dotProduct7uBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int dims,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment result
    ) {
        try {
            getVectorDistance().dotProductHandle7uBulkWithOffsets().invokeExact(a, b, dims, pitch, offsets, count, result);
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

    static void dotProductBulkScalar(byte[] query, byte[][] data, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = dotProductScalar(query, data[i]);
        }
    }

    static void dotProductBulkWithOffsetsScalar(byte[] query, byte[][] data, int[] offsets, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = dotProductScalar(query, data[offsets[i]]);
        }
    }

    static void assertScoresEquals(float[] expectedScores, MemorySegment expectedScoresSeg) {
        assert expectedScores.length == (expectedScoresSeg.byteSize() / Float.BYTES);
        for (int i = 0; i < expectedScores.length; i++) {
            assertEquals(expectedScores[i], expectedScoresSeg.get(JAVA_FLOAT_UNALIGNED, i * Float.BYTES), 0f);
        }
    }
}
