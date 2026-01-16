/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.ToIntBiFunction;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryInt4Tests extends VectorSimilarityFunctionsTests {

    private final byte indexBits = 1;
    private static final byte queryBits = 4;

    private final byte maxQueryValue = (1 << queryBits) - 1;
    private final byte maxIndexValue = (1 << indexBits) - 1;

    public JDKVectorLibraryInt4Tests(SimilarityFunction function, int size) {
        super(function, size);
    }

    @BeforeClass
    public static void beforeClass() {
        VectorSimilarityFunctionsTests.setup();
    }

    @AfterClass
    public static void afterClass() {
        VectorSimilarityFunctionsTests.cleanup();
    }

    public void testInt4BinaryVectors() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);

        int discretizedDimensions = discretizedDimensions(dims, indexBits);
        final int indexVectorBytes = getPackedLength(discretizedDimensions, indexBits);

        final int queryVectorBytes = indexVectorBytes * (queryBits / indexBits);

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVectors = new byte[numVecs][dims];

        var indexVectors = new byte[numVecs][indexVectorBytes];
        var queryVectors = new byte[numVecs][queryVectorBytes];

        var querySegment = arena.allocate((long) queryVectorBytes * numVecs);
        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);

        for (int i = 0; i < numVecs; i++) {

            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            randomBytesBetween(unpackedQueryVectors[i], (byte) 0, maxQueryValue);

            pack(unpackedIndexVectors[i], indexVectors[i], indexBits, indexVectorBytes);
            pack(unpackedQueryVectors[i], queryVectors[i], queryBits, indexVectorBytes);

            MemorySegment.copy(MemorySegment.ofArray(indexVectors[i]), 0L, indexSegment, (long) i * indexVectorBytes, indexVectorBytes);
            MemorySegment.copy(MemorySegment.ofArray(queryVectors[i]), 0L, querySegment, (long) i * queryVectorBytes, queryVectorBytes);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int queryIndex = randomInt(numVecs - 1);
            int indexIndex = randomInt(numVecs - 1);
            var querySlice = querySegment.asSlice((long) queryIndex * queryVectorBytes, queryVectorBytes);
            var indexSlice = indexSegment.asSlice((long) indexIndex * indexVectorBytes, indexVectorBytes);

            int expected = scalarSimilarity(unpackedQueryVectors[queryIndex], unpackedIndexVectors[indexIndex]);
            assertEquals(expected, nativeSimilarity(indexSlice, querySlice, indexVectorBytes));
            if (supportsHeapSegments()) {
                var queryHeapSegment = MemorySegment.ofArray(queryVectors[queryIndex]);
                var indexHeapSegment = MemorySegment.ofArray(indexVectors[indexIndex]);
                assertEquals(expected, nativeSimilarity(indexHeapSegment, queryHeapSegment, indexVectorBytes));
                assertEquals(expected, nativeSimilarity(indexHeapSegment, querySlice, indexVectorBytes));
                assertEquals(expected, nativeSimilarity(indexSlice, queryHeapSegment, indexVectorBytes));

                // trivial bulk with a single vector
                float[] bulkScore = new float[1];
                nativeSimilarityBulk(indexSlice, querySlice, indexVectorBytes, 1, MemorySegment.ofArray(bulkScore));
                assertEquals(expected, bulkScore[0], 0f);
            }
        }
    }

    public void testInt4Bulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);

        int discretizedDimensions = discretizedDimensions(dims, indexBits);
        final int indexVectorBytes = getPackedLength(discretizedDimensions, indexBits);

        final int queryVectorBytes = indexVectorBytes * (queryBits / indexBits);

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVector = new byte[dims];

        var indexVectors = new byte[numVecs][indexVectorBytes];
        var queryVector = new byte[queryVectorBytes];

        var querySegment = arena.allocate((long) queryVectorBytes);
        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        pack(unpackedQueryVector, queryVector, queryBits, indexVectorBytes);
        MemorySegment.copy(MemorySegment.ofArray(queryVector), 0L, querySegment, 0L, queryVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            pack(unpackedIndexVectors[i], indexVectors[i], indexBits, indexVectorBytes);
            MemorySegment.copy(MemorySegment.ofArray(indexVectors[i]), 0L, indexSegment, (long) i * indexVectorBytes, indexVectorBytes);
        }

        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulk(unpackedQueryVector, unpackedIndexVectors, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        nativeSimilarityBulk(indexSegment, querySegment, indexVectorBytes, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            nativeSimilarityBulk(indexSegment, querySegment, indexVectorBytes, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, 0f);
        }
    }

    public void testInt4BulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);

        int discretizedDimensions = discretizedDimensions(dims, indexBits);
        final int indexVectorBytes = getPackedLength(discretizedDimensions, indexBits);

        final int queryVectorBytes = indexVectorBytes * (queryBits / indexBits);

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVector = new byte[dims];

        var indexVectors = new byte[numVecs][indexVectorBytes];
        var queryVector = new byte[queryVectorBytes];
        var offsets = new int[numVecs];

        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        var querySegment = arena.allocate((long) queryVectorBytes);
        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        pack(unpackedQueryVector, queryVector, queryBits, indexVectorBytes);
        MemorySegment.copy(MemorySegment.ofArray(queryVector), 0L, querySegment, 0L, queryVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);

            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            pack(unpackedIndexVectors[i], indexVectors[i], indexBits, indexVectorBytes);
            MemorySegment.copy(MemorySegment.ofArray(indexVectors[i]), 0L, indexSegment, (long) i * indexVectorBytes, indexVectorBytes);
        }

        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedQueryVector, unpackedIndexVectors, offsets, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        nativeSimilarityBulkWithOffsets(
            indexSegment,
            querySegment,
            indexVectorBytes,
            indexVectorBytes,
            offsetsSegment,
            numVecs,
            bulkScoresSeg
        );
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);

        int discretizedDimensions = discretizedDimensions(dims, indexBits);
        final int indexVectorBytes = getPackedLength(discretizedDimensions, indexBits);

        final int queryVectorBytes = indexVectorBytes * (queryBits / indexBits);

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVector = new byte[dims];

        var indexVectors = new byte[numVecs][indexVectorBytes];
        var queryVector = new byte[queryVectorBytes];
        var offsets = new int[numVecs];

        // Mimics extra data at the end
        var pitch = indexVectorBytes + Float.BYTES;

        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);
        var querySegment = arena.allocate(queryVectorBytes);
        var indexSegment = arena.allocate((long) pitch * numVecs);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        pack(unpackedQueryVector, queryVector, queryBits, indexVectorBytes);
        MemorySegment.copy(MemorySegment.ofArray(queryVector), 0L, querySegment, 0L, queryVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);

            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            pack(unpackedIndexVectors[i], indexVectors[i], indexBits, indexVectorBytes);
            MemorySegment.copy(MemorySegment.ofArray(indexVectors[i]), 0L, indexSegment, (long) i * pitch, indexVectorBytes);
        }

        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedQueryVector, unpackedIndexVectors, offsets, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        nativeSimilarityBulkWithOffsets(indexSegment, querySegment, indexVectorBytes, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);

        int discretizedDimensions = discretizedDimensions(dims, indexBits);
        final int indexVectorBytes = getPackedLength(discretizedDimensions, indexBits);

        final int queryVectorBytes = indexVectorBytes * (queryBits / indexBits);

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVector = new byte[dims];

        var indexVectors = new byte[numVecs][indexVectorBytes];
        var queryVector = new byte[queryVectorBytes];
        var offsets = new int[numVecs];

        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        pack(unpackedQueryVector, queryVector, queryBits, indexVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);

            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            pack(unpackedIndexVectors[i], indexVectors[i], indexBits, indexVectorBytes);
            MemorySegment.copy(MemorySegment.ofArray(indexVectors[i]), 0L, indexSegment, (long) i * indexVectorBytes, indexVectorBytes);
        }

        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedQueryVector, unpackedIndexVectors, offsets, expectedScores);

        float[] bulkScores = new float[numVecs];
        nativeSimilarityBulkWithOffsets(
            indexSegment,
            MemorySegment.ofArray(queryVector),
            indexVectorBytes,
            indexVectorBytes,
            MemorySegment.ofArray(offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, 0f);
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3);

        var ex = expectThrows(IOOBE, () -> nativeSimilarity(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> nativeSimilarity(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segA = arena.allocate((long) size * 3);
        var segB = arena.allocate((long) size * 3);
        var segS = arena.allocate((long) size * Float.BYTES);

        Exception ex = expectThrows(IOOBE, () -> nativeSimilarityBulk(segA, segB, size, 4, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> nativeSimilarityBulk(segA, segB, size, -1, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> nativeSimilarityBulk(segA, segB, -1, 3, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmall = arena.allocate((long) 3 * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> nativeSimilarityBulk(segA, segB, size, 3, tooSmall));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    private void pack(byte[] unpackedVector, byte[] packedVector, byte elementBits, int pitch) {
        for (int i = 0; i < unpackedVector.length; i++) {
            var value = unpackedVector[i];
            var packedIndex = i / 8;
            var packedBitPosition = (7 - (i % 8));

            for (int j = 0; j < elementBits; ++j) {
                int v = value & 0x1;
                int shifted = v << packedBitPosition;
                value >>= 1;
                packedVector[packedIndex + j * pitch] += (byte) shifted;
            }
        }
    }

    private static int discretizedDimensions(int dimensions, int indexBits) {
        if (queryBits == indexBits) {
            int totalBits = dimensions * indexBits;
            return (totalBits + 7) / 8 * 8 / indexBits;
        }
        int queryDiscretized = (dimensions * queryBits + 7) / 8 * 8 / queryBits;
        int docDiscretized = (dimensions * indexBits + 7) / 8 * 8 / indexBits;
        int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
        assert maxDiscretized % (8.0 / queryBits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        assert maxDiscretized % (8.0 / indexBits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
        return maxDiscretized;
    }

    // Returns how many bytes do we need to store the quantized vector
    private static int getPackedLength(int discretizedDimensions, int bits) {
        int totalBits = discretizedDimensions * bits;
        return (totalBits + 7) / 8;
    }

    long nativeSimilarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return switch (function) {
                case DOT_PRODUCT -> (long) getVectorDistance().dotProductHandleI1I4().invokeExact(a, b, length);
                case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
            };
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void nativeSimilarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            switch (function) {
                case DOT_PRODUCT -> getVectorDistance().dotProductHandleI1I4Bulk().invokeExact(a, b, dims, count, result);
                case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void nativeSimilarityBulkWithOffsets(
        MemorySegment a,
        MemorySegment b,
        int dims,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment result
    ) {
        try {
            switch (function) {
                case DOT_PRODUCT -> getVectorDistance().dotProductHandleI1I4BulkWithOffsets()
                    .invokeExact(a, b, dims, pitch, offsets, count, result);
                case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int scalarSimilarity(byte[] a, byte[] b) {
        return switch (function) {
            case DOT_PRODUCT -> dotProductScalar(a, b);
            case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
        };
    }

    void scalarSimilarityBulk(byte[] query, byte[][] data, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkScalar(JDKVectorLibraryInt4Tests::dotProductScalar, query, data, scores);
            case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
        }
    }

    void scalarSimilarityBulkWithOffsets(byte[] query, byte[][] data, int[] offsets, float[] scores) {
        switch (function) {
            case DOT_PRODUCT -> bulkWithOffsetsScalar(JDKVectorLibraryInt4Tests::dotProductScalar, query, data, offsets, scores);
            case SQUARE_DISTANCE -> throw new AssumptionViolatedException("square distance not implemented");
        }
    }

    static int dotProductScalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
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
