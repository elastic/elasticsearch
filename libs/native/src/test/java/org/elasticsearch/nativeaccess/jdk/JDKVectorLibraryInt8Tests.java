/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.IntFunction;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryInt8Tests extends VectorSimilarityFunctionsTests {

    final float delta;

    public JDKVectorLibraryInt8Tests(VectorSimilarityFunctions.Function function, int size) {
        super(function, size);
        this.delta = 1e-5f * size; // scale the delta with the size
    }

    @BeforeClass
    public static void beforeClass() {
        VectorSimilarityFunctionsTests.setup();
    }

    @AfterClass
    public static void afterClass() {
        VectorSimilarityFunctionsTests.cleanup();
    }

    public void testAllZeroValues() {
        assumeFalse("Cosine is undefined for zero vectors", function == VectorSimilarityFunctions.Function.COSINE);
        testByteVectors(byte[]::new);
    }

    public void testRandomBytes() {
        testByteVectors(ESTestCase::randomByteArrayOfLength);
    }

    public void testByteVectors(IntFunction<byte[]> vectorGeneratorFunc) {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            values[i] = vectorGeneratorFunc.apply(dims);
            long dstOffset = (long) i * dims;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_BYTE, 0L, segment, JAVA_BYTE, dstOffset, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims, dims);
            var nativeSeg2 = segment.asSlice((long) second * dims, dims);

            float expected = scalarSimilarity(values[first], values[second]);
            assertEquals(expected, similarity(nativeSeg1, nativeSeg2, dims), delta);
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, similarity(heapSeg1, heapSeg2, dims), delta);
                assertEquals(expected, similarity(nativeSeg1, heapSeg2, dims), delta);
                assertEquals(expected, similarity(heapSeg1, nativeSeg2, dims), delta);
            }
        }
    }

    public void testByteBulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            values[i] = randomByteArrayOfLength(dims);
            long dstOffset = (long) i * dims;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_BYTE, 0L, segment, JAVA_BYTE, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulk(values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            similarityBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, delta);
        }
    }

    public void testByteBulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new byte[numVecs][];
        var vectorsSegment = arena.allocate((long) dims * numVecs);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            vectors[i] = randomByteArrayOfLength(dims);
            long dstOffset = (long) i * dims;
            MemorySegment.copy(MemorySegment.ofArray(vectors[i]), JAVA_BYTE, 0L, vectorsSegment, JAVA_BYTE, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, dims, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testByteBulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new byte[numVecs][];

        // Mimics extra data at the end
        var pitch = dims + Integer.BYTES;
        var vectorsSegment = arena.allocate((long) numVecs * pitch);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            vectors[i] = randomByteArrayOfLength(dims);
            long dstOffset = (long) i * pitch;
            MemorySegment.copy(MemorySegment.ofArray(vectors[i]), JAVA_BYTE, 0L, vectorsSegment, JAVA_BYTE, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * pitch, pitch);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testByteBulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var values = new byte[numVecs][];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            values[i] = randomByteArrayOfLength(dims);
            long dstOffset = (long) i * dims;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_BYTE, 0L, segment, JAVA_BYTE, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(values[queryOrd], values, offsets, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);

        float[] bulkScores = new float[numVecs];
        similarityBulkWithOffsets(
            segment,
            nativeQuerySeg,
            dims,
            dims,
            MemorySegment.ofArray(offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, delta);
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3);

        Exception ex = expectThrows(IAE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(ex.getMessage(), containsString("Dimensions differ"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    float similarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT8,
                VectorSimilarityFunctions.Operation.SINGLE
            ).invokeExact(a, b, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, VectorSimilarityFunctions.DataType.INT8, VectorSimilarityFunctions.Operation.BULK)
                .invokeExact(a, b, dims, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int dims,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment result
    ) {
        try {
            getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT8,
                VectorSimilarityFunctions.Operation.BULK_OFFSETS
            ).invokeExact(a, b, dims, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    float scalarSimilarity(byte[] a, byte[] b) {
        return switch (function) {
            case COSINE -> cosineScalar(a, b);
            case DOT_PRODUCT -> dotProductScalar(a, b);
            case SQUARE_DISTANCE -> squareDistanceScalar(a, b);
        };
    }

    void scalarSimilarityBulk(byte[] query, byte[][] data, float[] scores) {
        switch (function) {
            case COSINE -> bulkScalar(JDKVectorLibraryInt8Tests::cosineScalar, query, data, scores);
            case DOT_PRODUCT -> bulkScalar(JDKVectorLibraryInt8Tests::dotProductScalar, query, data, scores);
            case SQUARE_DISTANCE -> bulkScalar(JDKVectorLibraryInt8Tests::squareDistanceScalar, query, data, scores);
        }
    }

    void scalarSimilarityBulkWithOffsets(byte[] query, byte[][] data, int[] offsets, float[] scores) {
        switch (function) {
            case COSINE -> bulkWithOffsetsScalar(JDKVectorLibraryInt8Tests::cosineScalar, query, data, offsets, scores);
            case DOT_PRODUCT -> bulkWithOffsetsScalar(JDKVectorLibraryInt8Tests::dotProductScalar, query, data, offsets, scores);
            case SQUARE_DISTANCE -> bulkWithOffsetsScalar(JDKVectorLibraryInt8Tests::squareDistanceScalar, query, data, offsets, scores);
        }
    }

    /** Computes the cosine of the given vectors a and b. */
    static float cosineScalar(byte[] a, byte[] b) {
        int sum = 0;
        int norm1 = 0;
        int norm2 = 0;

        for (int i = 0; i < a.length; i++) {
            byte elem1 = a[i];
            byte elem2 = b[i];
            sum += elem1 * elem2;
            norm1 += elem1 * elem1;
            norm2 += elem2 * elem2;
        }
        return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
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
        int squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }

    @FunctionalInterface
    private interface Similarity {
        float function(byte[] a, byte[] b);
    }

    static void bulkScalar(Similarity function, byte[] query, byte[][] data, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = function.function(query, data[i]);
        }
    }

    static void bulkWithOffsetsScalar(Similarity function, byte[] query, byte[][] data, int[] offsets, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = function.function(query, data[offsets[i]]);
        }
    }

    void assertScoresEquals(float[] expectedScores, MemorySegment expectedScoresSeg) {
        assert expectedScores.length == (expectedScoresSeg.byteSize() / Float.BYTES);
        for (int i = 0; i < expectedScores.length; i++) {
            assertEquals(
                "Difference at offset " + i,
                expectedScores[i],
                expectedScoresSeg.get(JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES),
                delta
            );
        }
    }
}
