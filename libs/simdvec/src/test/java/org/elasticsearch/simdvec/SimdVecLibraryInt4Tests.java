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

import static org.elasticsearch.simdvec.Int4TestUtils.dotProductI4SinglePacked;
import static org.elasticsearch.simdvec.Int4TestUtils.packNibbles;
import static org.hamcrest.Matchers.containsString;

/**
 * Low-level tests for native Int4 (packed-nibble) dot product functions.
 *
 * <p>Int4 vectors are asymmetric: the "unpacked" query has {@code 2 * packedLen} bytes
 * (one value per byte, range 0-15), while the "packed" document has {@code packedLen} bytes
 * (two nibbles per byte). The third argument to native functions is {@code packedLen},
 * not the logical dimension count.
 */
public class SimdVecLibraryInt4Tests extends SimdVecLibraryTests {

    static final byte MIN_INT4_VALUE = 0;
    static final byte MAX_INT4_VALUE = 0x0F;

    public SimdVecLibraryInt4Tests(SimdVecLibrary.SimilarityFunction function, int size) {
        super(function, size);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(SimdVecLibraryTests.parametersFactory());
        // Int4 only supports dot product
        baseParams.removeIf(os -> os[0] != SimdVecLibrary.SimilarityFunction.DOT_PRODUCT);
        // Int4 requires even dimensions (two nibbles per packed byte)
        baseParams.removeIf(os -> (Integer) os[1] % 2 != 0);
        return baseParams;
    }

    @BeforeClass
    public static void beforeClass() {
        SimdVecLibraryTests.setup();
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

            var heapUnpacked = MemorySegment.ofArray(unpackedValues[first]);
            assertRejectsHeapSegments(() -> similarity(heapUnpacked, nativePacked, packedLen));

            // trivial bulk with a single vector
            float[] bulkScore = new float[1];
            similarityBulk(nativePacked, nativeUnpacked, packedLen, 1, MemorySegment.ofArray(bulkScore));
            assertEquals(expected, bulkScore[0], 0f);
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

        float[] bulkScores = new float[numVecs];
        similarityBulk(packedSegment, nativeQuerySeg, packedLen, numVecs, MemorySegment.ofArray(bulkScores));
        assertArrayEquals(expectedScores, bulkScores, 0f);
    }

    public void testInt4BulkMasked() {
        assumeTrue(notSupportedMsg(), supported());
        doTestInt4BulkMasked(0, false);
    }

    public void testInt4BulkMaskedWithPitch() {
        assumeTrue(notSupportedMsg(), supported());
        doTestInt4BulkMasked(Float.BYTES, false);
    }

    public void testInt4BulkMaskedRejectsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        doTestInt4BulkMasked(0, true);
    }

    private void doTestInt4BulkMasked(int extraData, boolean heapQueryAndScores) {
        final int dims = size;
        final int packedLen = dims / 2;
        final int pitch = packedLen + extraData;
        final int count = randomIntBetween(1, Integer.SIZE);
        final int mask = randomBulkMask(count);

        var unpackedValues = new byte[count][dims];
        var packedSegment = arena.allocate((long) count * pitch);
        for (int i = 0; i < count; i++) {
            randomBytesBetween(unpackedValues[i], MIN_INT4_VALUE, MAX_INT4_VALUE);
            MemorySegment.copy(packNibbles(unpackedValues[i]), 0, packedSegment, ValueLayout.JAVA_BYTE, (long) i * pitch, packedLen);
        }
        var unpackedQuery = new byte[dims];
        randomBytesBetween(unpackedQuery, MIN_INT4_VALUE, MAX_INT4_VALUE);

        // the dot product of the unpacked values is the one of the packed document with the unpacked query
        float[] expectedScores = unselectedScores(count);
        ScalarOperations.bulkMasked(SimdVecLibrary.SimilarityFunction.DOT_PRODUCT, unpackedQuery, unpackedValues, mask, expectedScores);

        if (heapQueryAndScores) {
            float[] bulkScores = unselectedScores(count);
            assertRejectsHeapSegments(
                () -> similarityBulkMasked(
                    packedSegment,
                    MemorySegment.ofArray(unpackedQuery),
                    packedLen,
                    pitch,
                    mask,
                    count,
                    MemorySegment.ofArray(bulkScores)
                )
            );
        } else {
            var nativeQuerySeg = arena.allocate(dims);
            MemorySegment.copy(unpackedQuery, 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
            var bulkScoresSeg = unselectedScoresSegment(count);
            similarityBulkMasked(packedSegment, nativeQuerySeg, packedLen, pitch, mask, count, bulkScoresSeg);
            assertScoresEquals(expectedScores, bulkScoresSeg);
        }
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
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, packedSegment.asSlice((long) ordinals[i] * packedLen, packedLen));
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
        var query = arena.allocate(size);
        var scores = arena.allocate((long) count * Float.BYTES);

        var dummyVec = arena.allocate(packedLen);
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < count; i++) {
            addresses.setAtIndex(ValueLayout.ADDRESS, i, dummyVec);
        }

        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * (count - 1), ValueLayout.ADDRESS.byteAlignment());
        Exception ex = expectThrows(IOOBE, () -> similarityBulkSparse(tooSmallAddrs, query, packedLen, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallQuery = arena.allocate(packedLen - 1);
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

    public void testBulkMaskedIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        final int packedLen = size / 2;
        final int count = 3;
        var packedSegment = arena.allocate((long) packedLen * count);
        var query = arena.allocate(size);
        var scores = arena.allocate((long) Integer.SIZE * Float.BYTES);

        // a vector past the end of the block is selected
        Exception ex = expectThrows(IAE, () -> similarityBulkMasked(packedSegment, query, packedLen, packedLen, 1 << count, count, scores));
        assertThat(ex.getMessage(), containsString("selects vectors beyond count"));

        // a block larger than the mask
        var largePackedSegment = arena.allocate((long) packedLen * (Integer.SIZE + 1));
        var largeScores = arena.allocate((long) (Integer.SIZE + 1) * Float.BYTES);
        ex = expectThrows(
            IOOBE,
            () -> similarityBulkMasked(largePackedSegment, query, packedLen, packedLen, 1, Integer.SIZE + 1, largeScores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // a block larger than the dataset
        ex = expectThrows(IOOBE, () -> similarityBulkMasked(packedSegment, query, packedLen, packedLen, 1, count + 1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    int similarity(MemorySegment unpacked, MemorySegment packed, int packedLen) {
        return getVectorDistance().dotProductI4(packed, unpacked, packedLen);
    }

    void similarityBulk(MemorySegment packedDocs, MemorySegment unpackedQuery, int packedLen, int count, MemorySegment result) {
        getVectorDistance().dotProductI4Bulk(packedDocs, unpackedQuery, packedLen, count, result);
    }

    void similarityBulkMasked(
        MemorySegment packedDocs,
        MemorySegment unpackedQuery,
        int packedLen,
        int pitch,
        int mask,
        int count,
        MemorySegment result
    ) {
        getVectorDistance().dotProductI4BulkMasked(packedDocs, unpackedQuery, packedLen, pitch, mask, count, result);
    }

    void similarityBulkSparse(MemorySegment addresses, MemorySegment unpackedQuery, int packedLen, int count, MemorySegment result) {
        getVectorDistance().dotProductI4BulkSparse(addresses, unpackedQuery, packedLen, count, result);
    }

    static void scalarSimilarityBulk(byte[] unpackedQuery, byte[][] packedData, float[] scores) {
        for (int i = 0; i < packedData.length; i++) {
            scores[i] = dotProductI4SinglePacked(unpackedQuery, packedData[i]);
        }
    }
}
