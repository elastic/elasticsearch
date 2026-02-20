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
import java.nio.ByteOrder;
import java.util.List;
import java.util.function.IntFunction;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryFloat32Tests extends VectorSimilarityFunctionsTests {

    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final float delta;

    public JDKVectorLibraryFloat32Tests(VectorSimilarityFunctions.Function function, int size) {
        super(function, size);
        this.delta = 1e-5f * size; // scale the delta with the size
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(VectorSimilarityFunctionsTests.parametersFactory());
        // cosine is not used on floats
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

    public void testAllZeroValues() {
        testFloat32Vectors(float[]::new);
    }

    public void testRandomFloats() {
        testFloat32Vectors(JDKVectorLibraryFloat32Tests::randomFloatArray);
    }

    public void testFloat32Vectors(IntFunction<float[]> vectorGeneratorFunc) {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new float[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            values[i] = vectorGeneratorFunc.apply(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_FLOAT_UNALIGNED, 0L, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims * Float.BYTES, (long) dims * Float.BYTES);
            var nativeSeg2 = segment.asSlice((long) second * dims * Float.BYTES, (long) dims * Float.BYTES);

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

    public void testFloat32Bulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new float[numVecs][];
        var segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            values[i] = randomFloatArray(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_FLOAT_UNALIGNED, 0L, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulk(values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            similarityBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, delta);
        }
    }

    public void testFloat32BulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new float[numVecs][];
        var vectorsSegment = arena.allocate((long) dims * numVecs * Float.BYTES);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            vectors[i] = randomFloatArray(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(
                MemorySegment.ofArray(vectors[i]),
                JAVA_FLOAT_UNALIGNED,
                0L,
                vectorsSegment,
                LAYOUT_LE_FLOAT,
                dstOffset,
                dims
            );
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, dims * Float.BYTES, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testFloat32BulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var vectors = new float[numVecs][];

        // Mimics extra data at the end
        var pitch = dims * Float.BYTES + Float.BYTES;
        var vectorsSegment = arena.allocate((long) numVecs * pitch);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            vectors[i] = randomFloatArray(dims);
            long dstOffset = (long) i * pitch;
            MemorySegment.copy(
                MemorySegment.ofArray(vectors[i]),
                JAVA_FLOAT_UNALIGNED,
                0L,
                vectorsSegment,
                LAYOUT_LE_FLOAT,
                dstOffset,
                dims
            );
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * pitch, pitch);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testFloat32BulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var values = new float[numVecs][];
        var segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            values[i] = randomFloatArray(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_FLOAT_UNALIGNED, 0L, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(values[queryOrd], values, offsets, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims * Float.BYTES, dims);

        float[] bulkScores = new float[numVecs];
        similarityBulkWithOffsets(
            segment,
            nativeQuerySeg,
            dims,
            dims * Float.BYTES,
            MemorySegment.ofArray(offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, delta);
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3 * Float.BYTES);

        Exception ex = expectThrows(IAE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(ex.getMessage(), containsString("Dimensions differ"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    static float[] randomFloatArray(int length) {
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    }

    float similarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.FLOAT32,
                VectorSimilarityFunctions.Operation.SINGLE
            ).invokeExact(a, b, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, VectorSimilarityFunctions.DataType.FLOAT32, VectorSimilarityFunctions.Operation.BULK)
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
                VectorSimilarityFunctions.DataType.FLOAT32,
                VectorSimilarityFunctions.Operation.BULK_OFFSETS
            ).invokeExact(a, b, dims, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    float scalarSimilarity(float[] a, float[] b) {
        return switch (function) {
            case DOT_PRODUCT -> dotProductScalar(a, b);
            case SQUARE_DISTANCE -> squareDistanceScalar(a, b);
            case COSINE -> throw new AssumptionViolatedException("cosine not supported");
        };
    }

    void scalarSimilarityBulk(float[] query, float[][] data, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkScalar(JDKVectorLibraryFloat32Tests::dotProductScalar, query, data, scores);
            case SQUARE_DISTANCE -> bulkScalar(JDKVectorLibraryFloat32Tests::squareDistanceScalar, query, data, scores);
        }
    }

    void scalarSimilarityBulkWithOffsets(float[] query, float[][] data, int[] offsets, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkWithOffsetsScalar(JDKVectorLibraryFloat32Tests::dotProductScalar, query, data, offsets, scores);
            case SQUARE_DISTANCE -> bulkWithOffsetsScalar(JDKVectorLibraryFloat32Tests::squareDistanceScalar, query, data, offsets, scores);
        }
    }

    /** Computes the dot product of the given vectors a and b. */
    static float dotProductScalar(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    /** Computes the dot product of the given vectors a and b. */
    static float squareDistanceScalar(float[] a, float[] b) {
        float squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }

    @FunctionalInterface
    private interface Similarity {
        float function(float[] a, float[] b);
    }

    static void bulkScalar(Similarity function, float[] query, float[][] data, float[] scores) {
        for (int i = 0; i < data.length; i++) {
            scores[i] = function.function(query, data[i]);
        }
    }

    static void bulkWithOffsetsScalar(Similarity function, float[] query, float[][] data, int[] offsets, float[] scores) {
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
