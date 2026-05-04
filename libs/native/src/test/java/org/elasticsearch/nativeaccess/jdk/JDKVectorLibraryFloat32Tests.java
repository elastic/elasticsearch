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
            MemorySegment.copy(values[i], 0, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims * Float.BYTES, (long) dims * Float.BYTES);
            var nativeSeg2 = segment.asSlice((long) second * dims * Float.BYTES, (long) dims * Float.BYTES);

            float expected = ScalarOperations.similarity(function, values[first], values[second]);
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
            MemorySegment.copy(values[i], 0, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulk(function, values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);

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
            MemorySegment.copy(vectors[i], 0, vectorsSegment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, dims * Float.BYTES, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
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
            MemorySegment.copy(vectors[i], 0, vectorsSegment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, vectors[queryOrd], vectors, offsets, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * pitch, pitch);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(vectorsSegment, nativeQuerySeg, dims, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
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
            MemorySegment.copy(values[i], 0, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, values[queryOrd], values, offsets, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);

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

    // Verifies that individual offset values are bounds-checked against the data segment.
    public void testBulkOffsetsOutOfRange() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = 3;
        final int pitch = dims * Float.BYTES;
        var vectorsSegment = arena.allocate((long) pitch * numVecs);
        var query = arena.allocate((long) dims * Float.BYTES);
        var scores = arena.allocate((long) numVecs * Float.BYTES);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, numVecs);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 2, 0);
        Exception ex = expectThrows(
            IOOBE,
            () -> similarityBulkWithOffsets(vectorsSegment, query, dims, pitch, offsetsSegment, numVecs, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, -1);
        ex = expectThrows(IOOBE, () -> similarityBulkWithOffsets(vectorsSegment, query, dims, pitch, offsetsSegment, numVecs, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    // Tests bulk sparse similarity where vector addresses are slices of a single contiguous segment,
    // verifying correct lookup and scoring via an address array with random ordinals.
    public void testFloat32BulkSparse() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var values = new float[numVecs][];
        var segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
            values[i] = randomFloatArray(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(values[i], 0, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, values[queryOrd], values, ordinals, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(
                ValueLayout.ADDRESS,
                i,
                segment.asSlice((long) ordinals[i] * dims * Float.BYTES, (long) dims * Float.BYTES)
            );
        }
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    // Tests bulk sparse similarity where each vector lives in its own independently allocated segment,
    // ensuring the sparse path handles non-contiguous (scattered) memory correctly.
    public void testFloat32BulkSparseScattered() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var values = new float[numVecs][];
        var segments = new MemorySegment[numVecs];
        for (int i = 0; i < numVecs; i++) {
            values[i] = randomFloatArray(dims);
            segments[i] = arena.allocate((long) dims * Float.BYTES);
            MemorySegment.copy(values[i], 0, segments[i], LAYOUT_LE_FLOAT, 0L, dims);
        }
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, values[queryOrd], values, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, segments[ordinals[i]]);
        }
        var nativeQuerySeg = segments[queryOrd];
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    // Verifies that bulk sparse similarity rejects invalid arguments (undersized segments,
    // negative dims/count) with appropriate out-of-bounds exceptions.
    public void testBulkSparseIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        int count = 3;
        var query = arena.allocate((long) size * Float.BYTES);
        var scores = arena.allocate((long) count * Float.BYTES);

        var dummyVec = arena.allocate((long) size * Float.BYTES);
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < count; i++) {
            addresses.setAtIndex(ValueLayout.ADDRESS, i, dummyVec);
        }

        // addresses segment too small for the given count
        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * (count - 1), ValueLayout.ADDRESS.byteAlignment());
        Exception ex = expectThrows(IOOBE, () -> similarityBulkSparse(tooSmallAddrs, query, size, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // query segment too small for the given dims
        var tooSmallQuery = arena.allocate((long) size * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, tooSmallQuery, size, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // result segment too small for the given count
        var tooSmallScores = arena.allocate((long) count * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, size, count, tooSmallScores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // negative count
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, size, -1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // negative dims
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, -1, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // null (zero) address in the addresses segment
        var zeroAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        ex = expectThrows(IAE, () -> similarityBulkSparse(zeroAddrs, query, size, count, scores));
        assertThat(ex.getMessage(), containsString("null"));
    }

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segA = arena.allocate((long) size * 3 * Float.BYTES);
        var segB = arena.allocate((long) size * 3 * Float.BYTES);
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

    void similarityBulkSparse(MemorySegment addresses, MemorySegment query, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.FLOAT32,
                VectorSimilarityFunctions.Operation.BULK_SPARSE
            ).invokeExact(addresses, query, dims, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
