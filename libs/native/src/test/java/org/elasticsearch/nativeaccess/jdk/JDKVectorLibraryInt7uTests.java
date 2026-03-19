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

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.function.ToIntBiFunction;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryInt7uTests extends VectorSimilarityFunctionsTests {

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    public JDKVectorLibraryInt7uTests(VectorSimilarityFunctions.Function function, int size) {
        super(function, size);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(VectorSimilarityFunctionsTests.parametersFactory());
        // cosine is not used on float vectors, and quantization is only used on floats
        baseParams.removeIf(os -> os[0] == VectorSimilarityFunctions.Function.COSINE);
        return baseParams;
    }

    @BeforeClass
    public static void beforeClass() {
        VectorSimilarityFunctionsTests.setup();
    }

    @AfterClass
    public static void afterClass() {
        VectorSimilarityFunctionsTests.cleanup();
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

            int expected = scalarSimilarity(values[first], values[second]);
            assertEquals(expected, similarity(nativeSeg1, nativeSeg2, dims));
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, similarity(heapSeg1, heapSeg2, dims));
                assertEquals(expected, similarity(nativeSeg1, heapSeg2, dims));
                assertEquals(expected, similarity(heapSeg1, nativeSeg2, dims));

                // trivial bulk with a single vector
                float[] bulkScore = new float[1];
                similarityBulk(nativeSeg1, nativeSeg2, dims, 1, MemorySegment.ofArray(bulkScore));
                assertEquals(expected, bulkScore[0], 0f);
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
        scalarSimilarityBulk(values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            similarityBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores));
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
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, dims, offsetsSegment, numVecs, bulkScoresSeg);
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
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * pitch, pitch);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, pitch, offsetsSegment, numVecs, bulkScoresSeg);
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
        assertArrayEquals(expectedScores, bulkScores, 0f);
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

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segA = arena.allocate((long) size * 3);
        var segB = arena.allocate((long) size * 3);
        var segS = arena.allocate((long) size * Float.BYTES);

        Exception ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, size, 4, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, size, -1, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, -1, 3, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmall = arena.allocate((long) 3 * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, size, 3, tooSmall));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    int similarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT7U,
                VectorSimilarityFunctions.Operation.SINGLE
            ).invokeExact(a, b, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, VectorSimilarityFunctions.DataType.INT7U, VectorSimilarityFunctions.Operation.BULK)
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
                VectorSimilarityFunctions.DataType.INT7U,
                VectorSimilarityFunctions.Operation.BULK_OFFSETS
            ).invokeExact(a, b, dims, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int scalarSimilarity(byte[] a, byte[] b) {
        return switch (function) {
            case DOT_PRODUCT -> dotProductScalar(a, b);
            case SQUARE_DISTANCE -> squareDistanceScalar(a, b);
            case COSINE -> throw new AssumptionViolatedException("cosine not supported");
        };
    }

    void scalarSimilarityBulk(byte[] query, byte[][] data, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkScalar(JDKVectorLibraryInt7uTests::dotProductScalar, query, data, scores);
            case SQUARE_DISTANCE -> bulkScalar(JDKVectorLibraryInt7uTests::squareDistanceScalar, query, data, scores);
        }
    }

    void scalarSimilarityBulkWithOffsets(byte[] query, byte[][] data, int[] offsets, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkWithOffsetsScalar(JDKVectorLibraryInt7uTests::dotProductScalar, query, data, offsets, scores);
            case SQUARE_DISTANCE -> bulkWithOffsetsScalar(JDKVectorLibraryInt7uTests::squareDistanceScalar, query, data, offsets, scores);
        }
    }

    static int dotProductScalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static int squareDistanceScalar(byte[] a, byte[] b) {
        // Note: this will not overflow if dim < 2^18, since max(byte * byte) = 2^14.
        int squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }

    static void bulkScalar(ToIntBiFunction<byte[], byte[]> function, byte[] query, byte[][] data, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = function.applyAsInt(query, data[i]);
        }
    }

    static void bulkWithOffsetsScalar(
        ToIntBiFunction<byte[], byte[]> function,
        byte[] query,
        byte[][] data,
        int[] offsets,
        float[] scores
    ) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = function.applyAsInt(query, data[offsets[i]]);
        }
    }

    static void assertScoresEquals(float[] expectedScores, MemorySegment expectedScoresSeg) {
        assert expectedScores.length == (expectedScoresSeg.byteSize() / Float.BYTES);
        for (int i = 0; i < expectedScores.length; i++) {
            assertEquals(expectedScores[i], expectedScoresSeg.get(JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES), 0f);
        }
    }
}
