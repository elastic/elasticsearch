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
import org.elasticsearch.nativeaccess.BBQTestUtils;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryBBQTests extends VectorSimilarityFunctionsTests {

    private final VectorSimilarityFunctions.BBQType type;

    private final byte maxQueryValue;
    private final byte maxIndexValue;

    public JDKVectorLibraryBBQTests(VectorSimilarityFunctions.BBQType type, VectorSimilarityFunctions.Function function, int size) {
        super(function, size);
        this.type = type;
        this.maxQueryValue = (byte) ((1 << type.queryBits()) - 1);
        this.maxIndexValue = (byte) ((1 << type.dataBits()) - 1);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> baseParams = CollectionUtils.iterableAsArrayList(VectorSimilarityFunctionsTests.parametersFactory());
        // BBQ only with dimensions a multiple of 8
        baseParams.removeIf(os -> (Integer) os[1] % 8 != 0);
        // cosine is not a thing on BBQ
        baseParams.removeIf(os -> os[0] == VectorSimilarityFunctions.Function.COSINE);
        // remove all square distance (not implemented yet)
        baseParams.removeIf(os -> os[0] == VectorSimilarityFunctions.Function.SQUARE_DISTANCE);

        return () -> Stream.of(VectorSimilarityFunctions.BBQType.values())
            .flatMap(bbq -> baseParams.stream().map(os -> CollectionUtils.concatLists(List.of(bbq), Arrays.asList(os))))
            .map(List::toArray)
            .iterator();
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

        final int indexVectorBytes = BBQTestUtils.numBytes(dims, type.dataBits());
        final int queryVectorBytes = BBQTestUtils.numBytes(dims, type.queryBits());

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVectors = new byte[numVecs][dims];

        var indexVectors = new byte[numVecs][];
        var queryVectors = new byte[numVecs][];

        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);
        var querySegment = arena.allocate((long) queryVectorBytes * numVecs);

        for (int i = 0; i < numVecs; i++) {

            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            randomBytesBetween(unpackedQueryVectors[i], (byte) 0, maxQueryValue);

            indexVectors[i] = BBQTestUtils.packStriped(unpackedIndexVectors[i], type.dataBits());
            queryVectors[i] = BBQTestUtils.packStriped(unpackedQueryVectors[i], type.queryBits());

            MemorySegment.copy(indexVectors[i], 0, indexSegment, ValueLayout.JAVA_BYTE, (long) i * indexVectorBytes, indexVectorBytes);
            MemorySegment.copy(queryVectors[i], 0, querySegment, ValueLayout.JAVA_BYTE, (long) i * queryVectorBytes, queryVectorBytes);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int queryIndex = randomInt(numVecs - 1);
            int indexIndex = randomInt(numVecs - 1);
            var querySlice = querySegment.asSlice((long) queryIndex * queryVectorBytes, queryVectorBytes);
            var indexSlice = indexSegment.asSlice((long) indexIndex * indexVectorBytes, indexVectorBytes);

            float expected = ScalarOperations.similarity(function, unpackedQueryVectors[queryIndex], unpackedIndexVectors[indexIndex]);
            assertEquals(expected, nativeSimilarity(indexSlice, querySlice, indexVectorBytes), 0f);

            if (supportsHeapSegments()) {
                var queryHeapSegment = MemorySegment.ofArray(queryVectors[queryIndex]);
                var indexHeapSegment = MemorySegment.ofArray(indexVectors[indexIndex]);
                assertEquals(expected, nativeSimilarity(indexHeapSegment, queryHeapSegment, indexVectorBytes), 0f);
                assertEquals(expected, nativeSimilarity(indexHeapSegment, querySlice, indexVectorBytes), 0f);
                assertEquals(expected, nativeSimilarity(indexSlice, queryHeapSegment, indexVectorBytes), 0f);

                // trivial bulk with a single vector
                float[] bulkScore = new float[1];
                nativeSimilarityBulk(indexSlice, querySlice, indexVectorBytes, 1, MemorySegment.ofArray(bulkScore));
                assertEquals(expected, bulkScore[0], 0f);
            }
        }
    }

    private record TestData(
        byte[] unpackedQueryVector,
        byte[] queryVector,
        MemorySegment querySegment,
        int queryVectorBytes,
        byte[][] unpackedIndexVectors,
        MemorySegment indexSegment,
        int indexVectorBytes
    ) {}

    private record TestOffsets(int[] offsets, MemorySegment offsetsSegment) {}

    static TestOffsets createTestOffsets(final int numVecs) {
        var offsets = new int[numVecs];
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        for (int i = 0; i < numVecs; i++) {
            offsets[i] = randomInt(numVecs - 1);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, i, offsets[i]);
        }
        return new TestOffsets(offsets, offsetsSegment);
    }

    static TestData createTestData(final int numVecs, final int dims, final VectorSimilarityFunctions.BBQType type, final long extraData) {
        final byte maxIndexValue = (byte) ((1 << type.dataBits()) - 1);
        final byte maxQueryValue = (byte) ((1 << type.queryBits()) - 1);

        final int indexVectorBytes = BBQTestUtils.numBytes(dims, type.dataBits());
        final int queryVectorBytes = BBQTestUtils.numBytes(dims, type.queryBits());

        var unpackedIndexVectors = new byte[numVecs][dims];
        var unpackedQueryVector = new byte[dims];

        var indexVectors = new byte[numVecs][];

        // Mimics extra data at the end
        var indexLineLength = indexVectorBytes + extraData;

        var indexSegment = arena.allocate(indexLineLength * numVecs);
        var querySegment = arena.allocate(queryVectorBytes);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        var queryVector = BBQTestUtils.packStriped(unpackedQueryVector, type.queryBits());
        MemorySegment.copy(queryVector, 0, querySegment, ValueLayout.JAVA_BYTE, 0L, queryVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            indexVectors[i] = BBQTestUtils.packStriped(unpackedIndexVectors[i], type.dataBits());
            MemorySegment.copy(indexVectors[i], 0, indexSegment, ValueLayout.JAVA_BYTE, (long) i * indexLineLength, indexVectorBytes);
        }

        return new TestData(
            unpackedQueryVector,
            queryVector,
            querySegment,
            queryVectorBytes,
            unpackedIndexVectors,
            indexSegment,
            indexVectorBytes
        );
    }

    static TestData createTestData(final int numVecs, final int dims, final VectorSimilarityFunctions.BBQType type) {
        return createTestData(numVecs, dims, type, 0);
    }

    public void testInt4Bulk() {
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);
        final TestData testData = createTestData(numVecs, size, type);

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulk(function, testData.unpackedQueryVector, testData.unpackedIndexVectors, expectedScores);

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        nativeSimilarityBulk(testData.indexSegment, testData.querySegment, testData.indexVectorBytes, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);

        if (supportsHeapSegments()) {
            float[] bulkScores = new float[numVecs];
            nativeSimilarityBulk(
                testData.indexSegment,
                testData.querySegment,
                testData.indexVectorBytes,
                numVecs,
                MemorySegment.ofArray(bulkScores)
            );
            assertArrayEquals(expectedScores, bulkScores, 0f);
        }
    }

    public void testInt4BulkWithOffsets() {
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);
        final TestData testData = createTestData(numVecs, size, type);
        final TestOffsets testOffsets = createTestOffsets(numVecs);

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(
            function,
            testData.unpackedQueryVector,
            testData.unpackedIndexVectors,
            testOffsets.offsets,
            expectedScores
        );

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        nativeSimilarityBulkWithOffsets(
            testData.indexSegment,
            testData.querySegment,
            testData.indexVectorBytes,
            testData.indexVectorBytes,
            testOffsets.offsetsSegment,
            numVecs,
            bulkScoresSeg
        );
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsAndPitch() {
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);

        final TestData testData = createTestData(numVecs, size, type, Float.BYTES);
        final TestOffsets testOffsets = createTestOffsets(numVecs);

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(
            function,
            testData.unpackedQueryVector,
            testData.unpackedIndexVectors,
            testOffsets.offsets,
            expectedScores
        );

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);

        nativeSimilarityBulkWithOffsets(
            testData.indexSegment,
            testData.querySegment,
            testData.indexVectorBytes,
            testData.indexVectorBytes + Float.BYTES,
            testOffsets.offsetsSegment,
            numVecs,
            bulkScoresSeg
        );
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testInt4BulkWithOffsetsHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Requires support for heap MemorySegments", supportsHeapSegments());
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);

        final TestData testData = createTestData(numVecs, size, type);
        final TestOffsets testOffsets = createTestOffsets(numVecs);

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(
            function,
            testData.unpackedQueryVector,
            testData.unpackedIndexVectors,
            testOffsets.offsets,
            expectedScores
        );

        float[] bulkScores = new float[numVecs];
        nativeSimilarityBulkWithOffsets(
            testData.indexSegment,
            MemorySegment.ofArray(testData.queryVector),
            testData.indexVectorBytes,
            testData.indexVectorBytes,
            MemorySegment.ofArray(testOffsets.offsets),
            numVecs,
            MemorySegment.ofArray(bulkScores)
        );
        assertArrayEquals(expectedScores, bulkScores, 0f);
    }

    public void testBulkSparse() {
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);
        final TestData testData = createTestData(numVecs, size, type);

        var ordinals = new int[numVecs];
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, testData.unpackedQueryVector, testData.unpackedIndexVectors, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(
                ValueLayout.ADDRESS,
                i,
                testData.indexSegment.asSlice((long) ordinals[i] * testData.indexVectorBytes, testData.indexVectorBytes)
            );
        }

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        nativeSimilarityBulkSparse(addressesSeg, testData.querySegment, testData.indexVectorBytes, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testBulkSparseScattered() {
        assumeTrue(notSupportedMsg(), supported());

        final int numVecs = randomIntBetween(2, 101);
        final int indexVectorBytes = BBQTestUtils.numBytes(size, type.dataBits());
        final int queryVectorBytes = BBQTestUtils.numBytes(size, type.queryBits());

        var unpackedIndexVectors = new byte[numVecs][size];
        var unpackedQueryVector = new byte[size];

        var indexSegments = new MemorySegment[numVecs];
        var querySegment = arena.allocate(queryVectorBytes);

        randomBytesBetween(unpackedQueryVector, (byte) 0, maxQueryValue);
        var queryVector = BBQTestUtils.packStriped(unpackedQueryVector, type.queryBits());
        MemorySegment.copy(queryVector, 0, querySegment, ValueLayout.JAVA_BYTE, 0L, queryVectorBytes);

        for (int i = 0; i < numVecs; i++) {
            randomBytesBetween(unpackedIndexVectors[i], (byte) 0, maxIndexValue);
            var indexVector = BBQTestUtils.packStriped(unpackedIndexVectors[i], type.dataBits());
            indexSegments[i] = arena.allocate(indexVectorBytes);
            MemorySegment.copy(indexVector, 0, indexSegments[i], ValueLayout.JAVA_BYTE, 0L, indexVectorBytes);
        }

        var ordinals = new int[numVecs];
        for (int i = 0; i < numVecs; i++) {
            ordinals[i] = randomInt(numVecs - 1);
        }

        float[] expectedScores = new float[numVecs];
        ScalarOperations.bulkWithOffsets(function, unpackedQueryVector, unpackedIndexVectors, ordinals, expectedScores);

        var addressesSeg = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < numVecs; i++) {
            addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, indexSegments[ordinals[i]]);
        }

        var bulkScoresSeg = arena.allocate((long) numVecs * Float.BYTES);
        nativeSimilarityBulkSparse(addressesSeg, querySegment, indexVectorBytes, numVecs, bulkScoresSeg);
        assertScoresEquals(expectedScores, bulkScoresSeg);
    }

    public void testBulkSparseIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        final int indexVectorBytes = BBQTestUtils.numBytes(size, type.dataBits());
        final int queryVectorBytes = BBQTestUtils.numBytes(size, type.queryBits());
        int count = 3;
        var query = arena.allocate(queryVectorBytes);
        var scores = arena.allocate((long) count * Float.BYTES);

        var dummyVec = arena.allocate(indexVectorBytes);
        var addresses = arena.allocate(ValueLayout.ADDRESS.byteSize() * count, ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < count; i++) {
            addresses.setAtIndex(ValueLayout.ADDRESS, i, dummyVec);
        }

        var tooSmallAddrs = arena.allocate(ValueLayout.ADDRESS.byteSize() * (count - 1), ValueLayout.ADDRESS.byteAlignment());
        Exception ex = expectThrows(IOOBE, () -> nativeSimilarityBulkSparse(tooSmallAddrs, query, indexVectorBytes, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallQuery = arena.allocate(queryVectorBytes - 1);
        ex = expectThrows(IOOBE, () -> nativeSimilarityBulkSparse(addresses, tooSmallQuery, indexVectorBytes, count, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        var tooSmallScores = arena.allocate((long) count * Float.BYTES - 1);
        ex = expectThrows(IOOBE, () -> nativeSimilarityBulkSparse(addresses, query, indexVectorBytes, count, tooSmallScores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        ex = expectThrows(IOOBE, () -> nativeSimilarityBulkSparse(addresses, query, indexVectorBytes, -1, scores));
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
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

    // Verifies that individual offset values are bounds-checked against the data segment.
    public void testBulkOffsetsOutOfRange() {
        assumeTrue(notSupportedMsg(), supported());
        final int indexVectorBytes = BBQTestUtils.numBytes(size, type.dataBits());
        final int queryVectorBytes = BBQTestUtils.numBytes(size, type.queryBits());
        final int numVecs = 3;
        var indexSegment = arena.allocate((long) indexVectorBytes * numVecs);
        var query = arena.allocate(queryVectorBytes);
        var scores = arena.allocate((long) numVecs * Float.BYTES);
        var offsetsSegment = arena.allocate((long) numVecs * Integer.BYTES);

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, numVecs);
        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 2, 0);
        Exception ex = expectThrows(
            IOOBE,
            () -> nativeSimilarityBulkWithOffsets(indexSegment, query, indexVectorBytes, indexVectorBytes, offsetsSegment, numVecs, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 1, -1);
        ex = expectThrows(
            IOOBE,
            () -> nativeSimilarityBulkWithOffsets(indexSegment, query, indexVectorBytes, indexVectorBytes, offsetsSegment, numVecs, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }

    long nativeSimilarity(MemorySegment a, MemorySegment b, int length) {
        try {
            return (long) getVectorDistance().getHandle(function, type, VectorSimilarityFunctions.Operation.SINGLE)
                .invokeExact(a, b, length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void nativeSimilarityBulk(MemorySegment a, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, type, VectorSimilarityFunctions.Operation.BULK).invokeExact(a, b, dims, count, result);
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
            getVectorDistance().getHandle(function, type, VectorSimilarityFunctions.Operation.BULK_OFFSETS)
                .invokeExact(a, b, dims, pitch, offsets, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    void nativeSimilarityBulkSparse(MemorySegment addresses, MemorySegment b, int dims, int count, MemorySegment result) {
        try {
            getVectorDistance().getHandle(function, type, VectorSimilarityFunctions.Operation.BULK_SPARSE)
                .invokeExact(addresses, b, dims, count, result);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
