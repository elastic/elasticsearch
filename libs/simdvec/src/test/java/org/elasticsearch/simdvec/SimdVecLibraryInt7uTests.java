/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.util.CollectionUtils;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class SimdVecLibraryInt7uTests extends SimdVecLibraryTests {

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    public SimdVecLibraryInt7uTests(SimdVecLibrary.SimilarityFunction function, int size) {
        super(function, size);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(SimdVecLibraryTests.parametersFactory());
        // cosine is not used on float vectors, and quantization is only used on floats
        baseParams.removeIf(os -> os[0] == SimdVecLibrary.SimilarityFunction.COSINE);
        return baseParams;
    }

    @BeforeClass
    public static void beforeClass() {
        SimdVecLibraryTests.setup();
    }

    public void testInt7BinaryVectors() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(values[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(values[i], 0, segment, ValueLayout.JAVA_BYTE, (long) i * dims, dims);
        }

        final int loopTimes = 1000;
        var bulkScoreSeg = arena.allocate(Float.BYTES);
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims, dims);
            var nativeSeg2 = segment.asSlice((long) second * dims, dims);

            float expected = ScalarOperations.similarity(function, values[first], values[second]);
            assertEquals(expected, similarity(nativeSeg1, nativeSeg2, dims), 0f);
            var heapSeg1 = MemorySegment.ofArray(values[first]);
            // only the dot product binding is @Critical; the others reject heap segments
            if (function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT) {
                assertEquals(expected, similarity(nativeSeg2, heapSeg1, dims), 0f);
            } else {
                assertRejectsHeapSegments(() -> similarity(nativeSeg2, heapSeg1, dims));
            }

            // trivial bulk with a single vector
            similarityBulk(nativeSeg1, nativeSeg2, dims, 1, bulkScoreSeg);
            assertScoresEquals(new float[] { expected }, bulkScoreSeg);
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
            MemorySegment.copy(values[i], 0, segment, ValueLayout.JAVA_BYTE, (long) i * dims, dims);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulk(function, values[queryOrd], values, expectedScores);

        var nativeQuerySeg = segment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(segment, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        float[] bulkScores = new float[numVecs];
        // only the dot product binding is @Critical; the others reject heap segments
        if (function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT) {
            similarityBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, 0f);
        } else {
            assertRejectsHeapSegments(() -> similarityBulk(segment, nativeQuerySeg, dims, numVecs, MemorySegment.ofArray(bulkScores)));
        }
    }

    public void testInt7uBulkMasked() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("bulk masked is only implemented for dot product", function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT);
        doTestInt7uBulkMasked(0, false);
    }

    public void testInt7uBulkMaskedWithPitch() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("bulk masked is only implemented for dot product", function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT);
        // Mimics extra data at the end
        doTestInt7uBulkMasked(Float.BYTES, false);
    }

    public void testInt7uBulkMaskedRejectsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("bulk masked is only implemented for dot product", function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT);
        doTestInt7uBulkMasked(0, true);
    }

    private void doTestInt7uBulkMasked(int extraData, boolean heapQueryAndScores) {
        final int dims = size;
        final int pitch = dims + extraData;
        final int count = randomIntBetween(1, Integer.SIZE);
        final int mask = randomBulkMask(count);

        var vectors = new byte[count][dims];
        var vectorsSegment = arena.allocate((long) count * pitch);
        for (int i = 0; i < count; i++) {
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(vectors[i], 0, vectorsSegment, ValueLayout.JAVA_BYTE, (long) i * pitch, dims);
        }
        var query = new byte[dims];
        randomBytesBetween(query, MIN_INT7_VALUE, MAX_INT7_VALUE);

        float[] expectedScores = unselectedScores(count);
        ScalarOperations.bulkMasked(function, query, vectors, mask, expectedScores);

        if (heapQueryAndScores) {
            float[] bulkScores = unselectedScores(count);
            assertRejectsHeapSegments(
                () -> similarityBulkMasked(
                    vectorsSegment,
                    MemorySegment.ofArray(query),
                    dims,
                    pitch,
                    mask,
                    count,
                    MemorySegment.ofArray(bulkScores)
                )
            );
        } else {
            var nativeQuerySeg = arena.allocate(dims);
            MemorySegment.copy(query, 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
            var bulkScoresSeg = unselectedScoresSegment(count);
            similarityBulkMasked(vectorsSegment, nativeQuerySeg, dims, pitch, mask, count, bulkScoresSeg);
            assertScoresEquals(expectedScores, bulkScoresSeg);
        }
    }

    // Tests bulk sparse similarity where vector addresses are slices of a single contiguous segment,
    // verifying correct lookup and scoring via an address array with random ordinals.
    public void testInt7uBulkSparse() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var vectors = new byte[numVecs][dims];
        var vectorsSegment = arena.allocate((long) dims * numVecs);
        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(vectors[i], 0, vectorsSegment, ValueLayout.JAVA_BYTE, (long) i * dims, dims);
        }
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, vectorsSegment.asSlice((long) ordinals[i] * dims, dims));
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkSparse(function, vectors[queryOrd], vectors, ordinals, expectedScores);

        var nativeQuerySeg = vectorsSegment.asSlice((long) queryOrd * dims, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    // Tests bulk sparse similarity where each vector lives in its own independently allocated segment,
    // ensuring the sparse path handles non-contiguous (scattered) memory correctly.
    public void testInt7uBulkSparseScattered() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var ordinals = new int[numVecs];
        var vectors = new byte[numVecs][dims];
        var segments = new MemorySegment[numVecs];
        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            segments[i] = arena.allocate(dims);
            MemorySegment.copy(vectors[i], 0, segments[i], ValueLayout.JAVA_BYTE, 0L, dims);
        }
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }
        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkSparse(function, vectors[queryOrd], vectors, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, segments[ordinals[i]]);
        }
        var nativeQuerySeg = segments[queryOrd];
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkSparse(addressesSeg, nativeQuerySeg, dims, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3);

        // Segments can differ in size and be larger than length: only length bytes are read
        var aTail = randomIntBetween(0, size);
        var bTail = randomIntBetween(0, size);
        similarity(segment.asSlice(0L, size + aTail), segment.asSlice(0L, size + bTail), size);

        Exception ex = expectThrows(IOOBE, () -> similarity(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
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

    // Verifies that bulk sparse similarity rejects invalid arguments (undersized segments,
    // negative dims/count) with appropriate out-of-bounds exceptions.
    public void testBulkSparseIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        int count = 3;
        var query = arena.allocate(size);
        var scores = arena.allocate((long) count * Float.BYTES);

        var dummyVec = arena.allocate(size);
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < count; i++) {
            addresses.setAtIndex(ValueLayout.ADDRESS, i, dummyVec);
        }

        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * count - 1);
        Exception ex = expectThrows(IOOBE, () -> similarityBulkSparse(tooSmallAddrs, query, size, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallQuery = arena.allocate(size - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, tooSmallQuery, size, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallScores = arena.allocate((long) count * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, size, count, tooSmallScores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, size, -1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, -1, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // null (zero) address in the addresses segment
        var zeroAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        ex = expectThrows(IAE, () -> similarityBulkSparse(zeroAddrs, query, size, count, scores));
        assertThat(ex.getMessage(), containsString("null"));
    }

    public void testBulkMaskedIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("bulk masked is only implemented for dot product", function == SimdVecLibrary.SimilarityFunction.DOT_PRODUCT);
        final int dims = size;
        final int count = 3;
        var vectorsSegment = arena.allocate((long) dims * count);
        var query = arena.allocate(dims);
        var scores = arena.allocate((long) Integer.SIZE * Float.BYTES);

        // a vector past the end of the block is selected
        Exception ex = expectThrows(IAE, () -> similarityBulkMasked(vectorsSegment, query, dims, dims, 1 << count, count, scores));
        assertThat(ex.getMessage(), containsString("selects vectors beyond count"));

        // a block larger than the mask
        var largeVectorsSegment = arena.allocate((long) dims * (Integer.SIZE + 1));
        var largeScores = arena.allocate((long) (Integer.SIZE + 1) * Float.BYTES);
        ex = expectThrows(IOOBE, () -> similarityBulkMasked(largeVectorsSegment, query, dims, dims, 1, Integer.SIZE + 1, largeScores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // a block larger than the dataset
        ex = expectThrows(IOOBE, () -> similarityBulkMasked(vectorsSegment, query, dims, dims, 1, count + 1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    int similarity(MemorySegment document, MemorySegment query, int length) {
        return switch (function) {
            case DOT_PRODUCT -> getVectorDistance().dotProductI7u(document, query, length);
            case SQUARE_DISTANCE -> getVectorDistance().squareDistanceI7u(document, query, length);
            default -> throw new UnsupportedOperationException(function.toString());
        };
    }

    void similarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        switch (function) {
            case DOT_PRODUCT -> getVectorDistance().dotProductI7uBulk(a, b, dims, count, result);
            case SQUARE_DISTANCE -> getVectorDistance().squareDistanceI7uBulk(a, b, dims, count, result);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }

    void similarityBulkMasked(MemorySegment a, MemorySegment b, int dims, int pitch, int mask, int count, MemorySegment result) {
        switch (function) {
            case DOT_PRODUCT -> getVectorDistance().dotProductI7uBulkMasked(a, b, dims, pitch, mask, count, result);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }

    void similarityBulkSparse(MemorySegment addresses, MemorySegment b, int dims, int count, MemorySegment result) {
        switch (function) {
            case DOT_PRODUCT -> getVectorDistance().dotProductI7uBulkSparse(addresses, b, dims, count, result);
            case SQUARE_DISTANCE -> getVectorDistance().squareDistanceI7uBulkSparse(addresses, b, dims, count, result);
            default -> throw new UnsupportedOperationException(function.toString());
        }
    }
}
