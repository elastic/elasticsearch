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
import java.util.List;

import static org.elasticsearch.nativeaccess.Int4TestUtils.dotProductI4SinglePacked;
import static org.elasticsearch.nativeaccess.Int4TestUtils.packNibbles;
import static org.hamcrest.Matchers.containsString;

/**
 * Low-level tests for native Int4 (packed-nibble) dot product functions.
 *
 * <p>Int4 vectors are asymmetric: the "unpacked" query has {@code 2 * packedLen} bytes
 * (one value per byte, range 0-15), while the "packed" document has {@code packedLen} bytes
 * (two nibbles per byte). The third argument to native functions is {@code packedLen},
 * not the logical dimension count.
 */
public class JDKVectorLibraryInt4Tests extends VectorSimilarityFunctionsTests {

    static final byte MIN_INT4_VALUE = 0;
    static final byte MAX_INT4_VALUE = 0x0F;

    public JDKVectorLibraryInt4Tests(VectorSimilarityFunctions.Function function, int size) {
        super(function, size);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(VectorSimilarityFunctionsTests.parametersFactory());
        // Int4 only supports dot product
        baseParams.removeIf(os -> os[0] != VectorSimilarityFunctions.Function.DOT_PRODUCT);
        // Int4 requires even dimensions (two nibbles per packed byte)
        baseParams.removeIf(os -> (Integer) os[1] % 2 != 0);
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

    public void testInt4BinaryVectors() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var unpackedSegment = arena.allocate((long) dims * numVecs);
        var packedSegment = arena.allocate((long) packedLen * numVecs);

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(unpackedValues[i], 0, unpackedSegment, ValueLayout.JAVA_BYTE, (long) i * dims, dims);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * packedLen, packedLen);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeUnpacked = unpackedSegment.asSlice((long) first * dims, dims);
            var nativePacked = packedSegment.asSlice((long) second * packedLen, packedLen);

            int expected = dotProductI4SinglePacked(unpackedValues[first], packedValues[second]);
            assertEquals(expected, similarity(nativeUnpacked, nativePacked, packedLen));

            if (supportsHeapSegments()) {
                var heapUnpacked = MemorySegment.ofArray(unpackedValues[first]);
                var heapPacked = MemorySegment.ofArray(packedValues[second]);
                assertEquals(expected, similarity(heapUnpacked, heapPacked, packedLen));
                assertEquals(expected, similarity(nativeUnpacked, heapPacked, packedLen));
                assertEquals(expected, similarity(heapUnpacked, nativePacked, packedLen));

                // trivial bulk with a single vector
                float[] bulkScore = new float[1];
                similarityBulk(nativePacked, nativeUnpacked, packedLen, 1, MemorySegment.ofArray(bulkScore));
                assertEquals(expected, bulkScore[0], 0f);
            }
        }
    }

    public void testInt4Bulk() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var packedSegment = arena.allocate((long) packedLen * numVecs);

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * packedLen, packedLen);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulk(unpackedValues[queryOrd], packedValues, expectedScores);

        var nativeQuerySeg = arena.allocate(dims);
        MemorySegment.copy(unpackedValues[queryOrd], 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulk(packedSegment, nativeQuerySeg, packedLen, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            similarityBulk(packedSegment, nativeQuerySeg, packedLen, numVecs, MemorySegment.ofArray(bulkScores));
            assertArrayEquals(expectedScores, bulkScores, 0f);
        }
    }

    public void testInt4BulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var offsets = new int[numVecs];
        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var packedSegment = arena.allocate((long) packedLen * numVecs);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * packedLen, packedLen);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedValues[queryOrd], packedValues, offsets, expectedScores);

        var nativeQuerySeg = arena.allocate(dims);
        MemorySegment.copy(unpackedValues[queryOrd], 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(packedSegment, nativeQuerySeg, packedLen, packedLen, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var offsets = new int[numVecs];
        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];

        int pitch = packedLen + Float.BYTES;
        var packedSegment = arena.allocate((long) numVecs * pitch);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * pitch, packedLen);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedValues[queryOrd], packedValues, offsets, expectedScores);

        var nativeQuerySeg = arena.allocate(dims);
        MemorySegment.copy(unpackedValues[queryOrd], 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        similarityBulkWithOffsets(packedSegment, nativeQuerySeg, packedLen, pitch, offsetsSegment, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var offsets = new int[numVecs];
        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var packedSegment = arena.allocate((long) packedLen * numVecs);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * packedLen, packedLen);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        scalarSimilarityBulkWithOffsets(unpackedValues[queryOrd], packedValues, offsets, expectedScores);

        float[] bulkScores = new float[numVecs];
        similarityBulkWithOffsets(
            packedSegment,
            MemorySegment.ofArray(unpackedValues[queryOrd]),
            packedLen,
            packedLen,
            MemorySegment.ofArray(offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, 0f);
    }

    public void testInt4BulkSparse() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var ordinals = new int[numVecs];
        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var packedSegment = arena.allocate((long) packedLen * numVecs);

        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            MemorySegment.copy(packedValues[i], 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * packedLen, packedLen);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        for (int i = 0; i < numVecs; i++) {
            expectedScores[i] = dotProductI4SinglePacked(unpackedValues[queryOrd], packedValues[ordinals[i]]);
        }

        var nativeQuerySeg = arena.allocate(dims);
        MemorySegment.copy(unpackedValues[queryOrd], 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(
                ValueLayout.ADDRESS,
                i,
                packedSegment.asSlice((long) ordinals[i] * packedLen, packedLen)
            );
        }

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, packedLen, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkSparseScattered() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int packedLen = dims / 2;
        final int numVecs = randomIntBetween(2, 101);

        var ordinals = new int[numVecs];
        var unpackedValues = new byte[numVecs][dims];
        var packedValues = new byte[numVecs][packedLen];
        var packedSegments = new MemorySegment[numVecs];

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            packedValues[i] = packNibbles(unpackedValues[i]);
            packedSegments[i] = arena.allocate(packedLen);
            MemorySegment.copy(packedValues[i], 0, packedSegments[i], ValueLayout.JAVA_BYTE, 0L, packedLen);
        }
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }

        int queryOrd = randomInt(numVecs - 1);
        float[] expectedScores = new float[numVecs];
        for (int i = 0; i < numVecs; i++) {
            expectedScores[i] = dotProductI4SinglePacked(unpackedValues[queryOrd], packedValues[ordinals[i]]);
        }

        var nativeQuerySeg = arena.allocate(dims);
        MemorySegment.copy(unpackedValues[queryOrd], 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, packedSegments[ordinals[i]]);
        }

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        similarityBulkSparse(addressesSeg, nativeQuerySeg, packedLen, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testBulkSparseIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        final int packedLen = size / 2;
        int count = 3;
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        var query = arena.allocate(size);
        var scores = arena.allocate((long) count * Float.BYTES);

        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * count - 1);
        Exception ex = expectThrows(IOOBE, () -> similarityBulkSparse(tooSmallAddrs, query, packedLen, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallQuery = arena.allocate(size - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, tooSmallQuery, packedLen, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallScores = arena.allocate((long) count * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, packedLen, count, tooSmallScores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulkSparse(addresses, query, packedLen, -1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        int packedLen = size / 2;
        var unpacked = arena.allocate((long) size);
        var packed = arena.allocate((long) packedLen + 1);

        var ex = expectThrows(IOOBE, () -> similarity(unpacked, packed.asSlice(0L, packedLen), packedLen + 1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarity(unpacked, packed.asSlice(0L, packedLen), -1));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    public void testBulkIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        int packedLen = size / 2;
        var segA = arena.allocate((long) packedLen - 1);
        var segB = arena.allocate(size);
        var segS = arena.allocate((long) size * Float.BYTES);

        Exception ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, packedLen, 4, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, packedLen, -1, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, -1, 3, segS));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmall = arena.allocate((long) 3 * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> similarityBulk(segA, segB, packedLen, 3, tooSmall));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    // Verifies that individual offset values are bounds-checked against the data segment.
    public void testBulkOffsetsOutOfRange() {
        assumeTrue(notSupportedMsg(), supported());
        final int packedLen = size / 2;
        // INT4 length is packedLen (bytes) not element count; checkBulkOffsets computes
        // rowBytes = packedLen * 4 / 8 which truncates to 0 when packedLen < 2.
        assumeTrue("INT4 bounds check requires packedLen >= 2", packedLen >= 2);
        final int numVecs = 3;
        var packedSegment = arena.allocate((long) packedLen * numVecs);
        var query = arena.allocate(size);
        var scores = arena.allocate((long) numVecs * Float.BYTES);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, numVecs);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 2, 0);
        Exception ex = expectThrows(
            IOOBE,
            () -> similarityBulkWithOffsets(packedSegment, query, packedLen, packedLen, offsetsSegment, numVecs, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, -1);
        ex = expectThrows(
            IOOBE,
            () -> similarityBulkWithOffsets(packedSegment, query, packedLen, packedLen, offsetsSegment, numVecs, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    int similarity(MemorySegment unpacked, MemorySegment packed, int packedLen) {
        try {
            return (int) getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT4,
                VectorSimilarityFunctions.Operation.SINGLE
            ).invokeExact(unpacked, packed, packedLen);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulk(MemorySegment packedDocs, MemorySegment unpackedQuery, int packedLen, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, VectorSimilarityFunctions.DataType.INT4, VectorSimilarityFunctions.Operation.BULK)
                .invokeExact(packedDocs, unpackedQuery, packedLen, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulkWithOffsets(
        MemorySegment packedDocs,
        MemorySegment unpackedQuery,
        int packedLen,
        int pitch,
        MemorySegment offsets,
        int count,
        MemorySegment result
    ) {
        try {
            getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT4,
                VectorSimilarityFunctions.Operation.BULK_OFFSETS
            ).invokeExact(packedDocs, unpackedQuery, packedLen, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void similarityBulkSparse(MemorySegment addresses, MemorySegment unpackedQuery, int packedLen, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(
                function,
                VectorSimilarityFunctions.DataType.INT4,
                VectorSimilarityFunctions.Operation.BULK_SPARSE
            ).invokeExact(addresses, unpackedQuery, packedLen, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static void scalarSimilarityBulk(byte[] unpackedQuery, byte[][] packedData, float[] scores) {
        for (int i = 0; i < packedData.length; i++) {
            scores[i] = dotProductI4SinglePacked(unpackedQuery, packedData[i]);
        }
    }

    static void scalarSimilarityBulkWithOffsets(byte[] unpackedQuery, byte[][] packedData, int[] offsets, float[] scores) {
        for (int i = 0; i < packedData.length; i++) {
            scores[i] = dotProductI4SinglePacked(unpackedQuery, packedData[offsets[i]]);
        }
    }
}
