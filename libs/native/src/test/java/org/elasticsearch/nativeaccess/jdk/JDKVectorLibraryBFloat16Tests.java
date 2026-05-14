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
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryBFloat16Tests extends VectorSimilarityFunctionsTests {

    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfShort LAYOUT_LE_BFLOAT16 = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final VectorSimilarityFunctions.BFloat16QueryType queryType;
    final float delta;

    public JDKVectorLibraryBFloat16Tests(
        VectorSimilarityFunctions.BFloat16QueryType queryType,
        VectorSimilarityFunctions.Function function,
        int size
    ) {
        super(function, size);
        this.queryType = queryType;
        this.delta = 1e-2f * size; // scale the delta with the size, bfloat16 has less precision
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(VectorSimilarityFunctionsTests.parametersFactory());
        // cosine is not used on bfloat16
        baseParams.removeIf(os -> os[0] == VectorSimilarityFunctions.Function.COSINE);
        return Arrays.stream(VectorSimilarityFunctions.BFloat16QueryType.values())
            .flatMap(q -> baseParams.stream().map(os -> CollectionUtils.concatLists(List.of(q), Arrays.asList(os)).toArray()))
            .toList();
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
        testBFloat16Vectors(float[]::new);
    }

    public void testRandomFloats() {
        testBFloat16Vectors(JDKVectorLibraryBFloat16Tests::randomBFloat16Array);
    }

    private void testBFloat16Vectors(IntFunction<float[]> vectorGeneratorFunc) {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new float[numVecs][];
        // create both anyway, regardless of query type
        var f32Segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        var bf16Segment = arena.allocate((long) dims * numVecs * BFloat16.BYTES);
        for (int i = 0; i < numVecs; i++) {
            values[i] = vectorGeneratorFunc.apply(dims);
            MemorySegment.copy(values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
            copyToBFloat16Segment(values[i], bf16Segment, (long) i * dims * BFloat16.BYTES);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = bf16Segment.asSlice((long) first * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES);
            var nativeSeg2 = switch (queryType) {
                case BFLOAT16 -> bf16Segment.asSlice((long) second * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES);
                case FLOAT32 -> f32Segment.asSlice((long) second * dims * Float.BYTES, (long) dims * Float.BYTES);
            };

            float expected = ScalarOperations.similarity(function, values[first], values[second]);
            assertEquals(expected, similarity(nativeSeg1, nativeSeg2, dims), delta);
        }
    }

    public void testBFloat16Bulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var f32Values = new float[numVecs][];
        var bf16Values = new float[numVecs][];
        var f32Segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        var bf16Segment = arena.allocate((long) dims * numVecs * BFloat16.BYTES);
        for (int i = 0; i < numVecs; i++) {
            f32Values[i] = randomFloatArray(dims);
            bf16Values[i] = truncateFloatArray(f32Values[i]);
            MemorySegment.copy(f32Values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
            copyToBFloat16Segment(bf16Values[i], bf16Segment, (long) i * dims * BFloat16.BYTES);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] queryVector;
        MemorySegment nativeQuerySeg = switch (queryType) {
            case BFLOAT16 -> {
                queryVector = bf16Values[queryOrd];
                yield bf16Segment.asSlice((long) queryOrd * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES);
            }
            case FLOAT32 -> {
                queryVector = f32Values[queryOrd];
                yield f32Segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
            }
        };

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulk(function, queryVector, bf16Values, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(bf16Segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    public void testBFloat16BulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        var f32Values = new float[numVecs][];
        var bf16Values = new float[numVecs][];
        var f32Segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        var bf16Segment = arena.allocate((long) dims * numVecs * BFloat16.BYTES);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            f32Values[i] = randomFloatArray(dims);
            bf16Values[i] = truncateFloatArray(f32Values[i]);
            MemorySegment.copy(f32Values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
            copyToBFloat16Segment(bf16Values[i], bf16Segment, (long) i * dims * BFloat16.BYTES);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] queryVector;
        MemorySegment nativeQuerySeg = switch (queryType) {
            case BFLOAT16 -> {
                queryVector = bf16Values[queryOrd];
                yield bf16Segment.asSlice((long) queryOrd * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES);
            }
            case FLOAT32 -> {
                queryVector = f32Values[queryOrd];
                yield f32Segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
            }
        };

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, queryVector, bf16Values, offsets, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkWithOffsets(bf16Segment, nativeQuerySeg, dims, dims * BFloat16.BYTES, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    public void testBFloat16BulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var offsets = new int[numVecs];
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        // Mimics extra data at the end
        // f32 doesn't need to be pitched like this, as its only used for individual queries
        var bf16Pitch = dims * BFloat16.BYTES + BFloat16.BYTES;
        var f32Values = new float[numVecs][];
        var bf16Values = new float[numVecs][];
        var f32Segment = arena.allocate((long) numVecs * dims * Float.BYTES);
        var bf16Segment = arena.allocate((long) numVecs * bf16Pitch);
        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            f32Values[i] = randomFloatArray(dims);
            bf16Values[i] = truncateFloatArray(f32Values[i]);
            MemorySegment.copy(f32Values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
            copyToBFloat16Segment(bf16Values[i], bf16Segment, (long) i * bf16Pitch);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] queryVector;
        MemorySegment nativeQuerySeg = switch (queryType) {
            case BFLOAT16 -> {
                queryVector = bf16Values[queryOrd];
                yield bf16Segment.asSlice((long) queryOrd * bf16Pitch, (long) dims * BFloat16.BYTES);
            }
            case FLOAT32 -> {
                queryVector = f32Values[queryOrd];
                yield f32Segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
            }
        };
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, queryVector, bf16Values, offsets, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(bf16Segment, nativeQuerySeg, dims, bf16Pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    // Tests bulk sparse similarity where vector addresses are slices of a single contiguous segment,
    // verifying correct lookup and scoring via an address array with random ordinals.
    public void testBFloat16BulkSparse() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var f32Values = new float[numVecs][];
        var bf16Values = new float[numVecs][];
        var f32Segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        var bf16Segment = arena.allocate((long) dims * numVecs * BFloat16.BYTES);
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
            f32Values[i] = randomFloatArray(dims);
            bf16Values[i] = truncateFloatArray(f32Values[i]);
            MemorySegment.copy(f32Values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
            copyToBFloat16Segment(bf16Values[i], bf16Segment, (long) i * dims * BFloat16.BYTES);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] queryVector;
        MemorySegment nativeQuerySeg = switch (queryType) {
            case BFLOAT16 -> {
                queryVector = bf16Values[queryOrd];
                yield bf16Segment.asSlice((long) queryOrd * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES);
            }
            case FLOAT32 -> {
                queryVector = f32Values[queryOrd];
                yield f32Segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
            }
        };

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, queryVector, bf16Values, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(
                ValueLayout.ADDRESS,
                i,
                bf16Segment.asSlice((long) ordinals[i] * dims * BFloat16.BYTES, (long) dims * BFloat16.BYTES)
            );
        }
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    // Tests bulk sparse similarity where each vector lives in its own independently allocated segment,
    // ensuring the sparse path handles non-contiguous (scattered) memory correctly.
    public void testBFloat16BulkSparseScattered() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var f32Values = new float[numVecs][];
        var bf16Values = new float[numVecs][];
        var bf16Segments = new MemorySegment[numVecs];
        var f32Segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            f32Values[i] = randomFloatArray(dims);
            bf16Values[i] = truncateFloatArray(f32Values[i]);
            bf16Segments[i] = arena.allocate((long) dims * BFloat16.BYTES);
            copyToBFloat16Segment(bf16Values[i], bf16Segments[i], 0L);
            MemorySegment.copy(f32Values[i], 0, f32Segment, LAYOUT_LE_FLOAT, (long) i * dims * Float.BYTES, dims);
        }
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] queryVector;
        MemorySegment nativeQuerySeg = switch (queryType) {
            case BFLOAT16 -> {
                queryVector = bf16Values[queryOrd];
                yield bf16Segments[queryOrd];
            }
            case FLOAT32 -> {
                queryVector = f32Values[queryOrd];
                yield f32Segment.asSlice((long) queryOrd * dims * Float.BYTES, (long) dims * Float.BYTES);
            }
        };

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, queryVector, bf16Values, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, bf16Segments[ordinals[i]]);
        }
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg, delta);
    }

    // Verifies that bulk sparse similarity rejects invalid arguments (undersized segments,
    // negative dims/count) with appropriate out-of-bounds exceptions.
    public void testBulkSparseIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        int count = 3;
        int queryElementSize = switch (queryType) {
            case BFLOAT16 -> BFloat16.BYTES;
            case FLOAT32 -> Float.BYTES;
        };
        var query = arena.allocate((long) size * queryElementSize);
        var scores = arena.allocate((long) count * Float.BYTES);

        var dummyVec = arena.allocate((long) size * BFloat16.BYTES);
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < count; i++) {
            addresses.setAtIndex(ValueLayout.ADDRESS, i, dummyVec);
        }

        // addresses segment too small for the given count
        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * (count - 1), ValueLayout.ADDRESS.byteAlignment());
        Exception ex = expectThrows(IOOBE, () -> similarityBulkSparse(tooSmallAddrs, query, size, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // query segment too small for the given dims
        var tooSmallQuery = arena.allocate((long) size * queryElementSize - 1);
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

    // Verifies that individual offset values are bounds-checked against the data segment.
    public void testBulkOffsetsOutOfRange() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = 3;
        final int pitch = dims * BFloat16.BYTES;
        var vectorsSegment = arena.allocate((long) pitch * numVecs);
        var query = switch (queryType) {
            case BFLOAT16 -> arena.allocate((long) dims * BFloat16.BYTES);
            case FLOAT32 -> arena.allocate((long) dims * Float.BYTES);
        };
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

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segA = arena.allocate((long) size * 3 * BFloat16.BYTES);
        var segB = arena.allocate((long) size * 3 * BFloat16.BYTES);
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

        int aSize = size * BFloat16.BYTES;
        int bSize = switch (queryType) {
            case BFLOAT16 -> size * BFloat16.BYTES;
            case FLOAT32 -> size * Float.BYTES;
        };

        Exception ex = expectThrows(IAE, () -> similarity(segment.asSlice(0L, aSize), segment.asSlice(0L, bSize + 2), size));
        assertThat(ex.getMessage(), containsString("Dimensions differ"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, aSize), segment.asSlice(bSize, bSize), size + 1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, aSize), segment.asSlice(bSize, bSize), -1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    static float[] truncateFloatArray(float[] array) {
        float[] bf = array.clone();
        for (int i = 0; i < bf.length; i++) {
            bf[i] = BFloat16.truncateToBFloat16(bf[i]);
        }
        return bf;
    }

    static float[] randomBFloat16Array(int length) {
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = BFloat16.truncateToBFloat16(randomFloat());
        }
        return fa;
    }

    private static void copyToBFloat16Segment(float[] fa, MemorySegment segment, long offset) {
        for (int i = 0; i < fa.length; i++) {
            short bfloat16 = BFloat16.floatToBFloat16(fa[i]);
            segment.set(LAYOUT_LE_BFLOAT16, offset + (long) i * BFloat16.BYTES, bfloat16);
        }
    }

    float similarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().getBFloat16Handle(function, queryType, VectorSimilarityFunctions.Operation.SINGLE)
                .invokeExact(a, b, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getBFloat16Handle(function, queryType, VectorSimilarityFunctions.Operation.BULK)
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
            getVectorDistance().getBFloat16Handle(function, queryType, VectorSimilarityFunctions.Operation.BULK_OFFSETS)
                .invokeExact(a, b, dims, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulkSparse(MemorySegment addresses, MemorySegment query, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getBFloat16Handle(function, queryType, VectorSimilarityFunctions.Operation.BULK_SPARSE)
                .invokeExact(addresses, query, dims, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
