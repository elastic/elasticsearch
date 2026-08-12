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

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
import org.elasticsearch.nativeaccess.BBQTestUtils;
import org.elasticsearch.nativeaccess.GuardPageAllocator;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.SimdVecLibrary;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.nativeaccess.SimdVecLibrary.SimilarityFunction.DOT_PRODUCT;
import static org.elasticsearch.nativeaccess.SimdVecLibraryTests.notSupportedMsg;
import static org.elasticsearch.nativeaccess.SimdVecLibraryTests.platformMsg;
import static org.elasticsearch.nativeaccess.SimdVecLibraryTests.supported;

/**
 * Tests that the native BBQ vector kernels do not read past the end of their input buffers.
 *
 * <p>Every doc and query vector is allocated flush against a guard page, so a read of even one byte past
 * the vector faults and crashes the JVM rather than silently returning a wrong score. This is a regression
 * test for the over-read fixed in #154179, where the byte-tail loop of the BBQ {@code dotProduct} kernels
 * did 8-byte loads while advancing one byte at a time. Reintroducing that bug makes this class crash on
 * every run, on every platform where {@link GuardPageAllocator#isSupported()}.
 *
 * <p>Only the tail matters for this bug class, so the dimension counts below are chosen to leave every
 * possible number of bytes over after the kernels' whole-chunk loop, rather than the round dimension
 * counts the other BBQ tests use.
 */
public class JDKVectorLibraryBBQGuardPageTests extends ESTestCase {

    static {
        NodeNamePatternConverter.setGlobalNodeName("guard-page-test");
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    /**
     * Dimension counts under test. All striped BBQ layouts hold {@code dims/8} bytes per bit plane, so
     * {@code 8..320} in steps of 8 leaves 1 to 40 bytes per plane after a whole-chunk loop of 16 bytes
     * (NEON/SSE) or 32 bytes (AVX2); the larger entries do the same for the 64 byte chunks of AVX-512.
     */
    private static final int[] DIMS = IntStream.concat(
        IntStream.rangeClosed(1, 40).map(i -> i * 8),
        IntStream.of(512, 520, 1016, 1024, 1032)
    ).toArray();

    private static SimdVecLibrary library;

    private final SimdVecLibrary.BBQType type;

    private final byte maxDocValue;
    private final byte maxQueryValue;

    public JDKVectorLibraryBBQGuardPageTests(SimdVecLibrary.BBQType type) {
        this.type = type;
        this.maxDocValue = (byte) ((1 << type.dataBits()) - 1);
        this.maxQueryValue = (byte) ((1 << type.queryBits()) - 1);
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Stream.of(SimdVecLibrary.BBQType.values()).map(type -> new Object[] { type }).iterator();
    }

    @BeforeClass
    public static void beforeClass() {
        // a pass here must mean the guard page was real, so skip rather than run unguarded
        assumeTrue("guard pages are not supported on this platform", GuardPageAllocator.isSupported());

        var simdVecSupported = supported();
        if (simdVecSupported) {
            var vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions();
            assertTrue("native vector library must be available on [" + platformMsg() + "]", vectorSimilarityFunctions.isPresent());
            library = vectorSimilarityFunctions.get();
        }
        assumeTrue(notSupportedMsg(), simdVecSupported);
    }

    @AfterClass
    public static void afterClass() {
        library = null;
    }

    public void testSinglePair() {
        try (var arena = GuardPageAllocator.ofConfined()) {
            for (int dims : DIMS) {
                int documentBytes = BBQTestUtils.numBytes(dims, type.dataBits());

                var unpackedDoc = randomUnpacked(dims, maxDocValue);
                var unpackedQuery = randomUnpacked(dims, maxQueryValue);

                var docSegment = arena.allocateAtPageEnd(packDoc(unpackedDoc));
                var querySegment = arena.allocateAtPageEnd(packQuery(unpackedQuery));

                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("dims=" + dims, expected, (float) dotProduct(docSegment, querySegment, documentBytes), 0f);
            }
        }
    }

    public void testBulk() {
        try (var arena = GuardPageAllocator.ofConfined()) {
            for (int dims : DIMS) {
                int documentBytes = BBQTestUtils.numBytes(dims, type.dataBits());
                int numVecs = randomIntBetween(2, 8);

                var unpackedDocs = new byte[numVecs][];
                var dataset = new byte[numVecs * documentBytes];
                for (int i = 0; i < numVecs; i++) {
                    unpackedDocs[i] = randomUnpacked(dims, maxDocValue);
                    System.arraycopy(packDoc(unpackedDocs[i]), 0, dataset, i * documentBytes, documentBytes);
                }
                var unpackedQuery = randomUnpacked(dims, maxQueryValue);

                var datasetSegment = arena.allocateAtPageEnd(dataset);
                var querySegment = arena.allocateAtPageEnd(packQuery(unpackedQuery));
                var scoresSegment = arena.allocate((long) numVecs * Float.BYTES);

                dotProductBulk(datasetSegment, querySegment, documentBytes, numVecs, scoresSegment);

                var expectedScores = new float[numVecs];
                ScalarOperations.bulk(DOT_PRODUCT, unpackedQuery, unpackedDocs, expectedScores);
                assertScores(dims, expectedScores, scoresSegment);
            }
        }
    }

    public void testBulkSparse() {
        assumeTrue("bulk sparse is only used in production for D1Q4 (BBQ)", type == SimdVecLibrary.BBQType.D1Q4);

        try (var arena = GuardPageAllocator.ofConfined()) {
            for (int dims : DIMS) {
                int documentBytes = BBQTestUtils.numBytes(dims, type.dataBits());
                int numVecs = randomIntBetween(2, 8);

                // one guard page per doc vector, so an over-read of any of them faults, not just of the last
                var unpackedDocs = new byte[numVecs][];
                var addressesSegment = arena.allocate(ValueLayout.ADDRESS.byteSize() * numVecs, ValueLayout.ADDRESS.byteAlignment());
                for (int i = 0; i < numVecs; i++) {
                    unpackedDocs[i] = randomUnpacked(dims, maxDocValue);
                    addressesSegment.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateAtPageEnd(packDoc(unpackedDocs[i])));
                }
                var unpackedQuery = randomUnpacked(dims, maxQueryValue);

                var querySegment = arena.allocateAtPageEnd(packQuery(unpackedQuery));
                var scoresSegment = arena.allocate((long) numVecs * Float.BYTES);

                library.dotProductD1Q4BulkSparse(addressesSegment, querySegment, documentBytes, numVecs, scoresSegment);

                var expectedScores = new float[numVecs];
                ScalarOperations.bulk(DOT_PRODUCT, unpackedQuery, unpackedDocs, expectedScores);
                assertScores(dims, expectedScores, scoresSegment);
            }
        }
    }

    private static byte[] randomUnpacked(int dims, byte maxValue) {
        var unpacked = new byte[dims];
        randomBytesBetween(unpacked, (byte) 0, maxValue);
        return unpacked;
    }

    private static void assertScores(int dims, float[] expectedScores, MemorySegment actualScores) {
        for (int i = 0; i < expectedScores.length; i++) {
            float actual = actualScores.get(ValueLayout.JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES);
            assertEquals("dims=" + dims + ", vector=" + i, expectedScores[i], actual, 0f);
        }
    }

    /** Layout-aware doc packer for the {@link #type} of this test instance. */
    private byte[] packDoc(byte[] unpacked) {
        return switch (type.layout()) {
            case STRIPED -> BBQTestUtils.packStriped(unpacked, type.dataBits());
            case PACKED -> BBQTestUtils.packQuads(unpacked);
        };
    }

    /** Layout-aware query packer for the {@link #type} of this test instance. */
    private byte[] packQuery(byte[] unpacked) {
        return switch (type.layout()) {
            case STRIPED -> BBQTestUtils.packStriped(unpacked, type.queryBits());
            case PACKED -> unpacked.clone();
        };
    }

    private long dotProduct(MemorySegment doc, MemorySegment query, int documentBytes) {
        return switch (type) {
            case D1Q1 -> library.dotProductD1Q1(doc, query, documentBytes);
            case D1Q4 -> library.dotProductD1Q4(doc, query, documentBytes);
            case D2Q2 -> library.dotProductD2Q2(doc, query, documentBytes);
            case D2Q4 -> library.dotProductD2Q4(doc, query, documentBytes);
            case D4Q4 -> library.dotProductD4Q4(doc, query, documentBytes);
            case D2Q4_PACKED -> library.dotProductD2Q4Packed(doc, query, documentBytes);
        };
    }

    private void dotProductBulk(MemorySegment dataset, MemorySegment query, int documentBytes, int count, MemorySegment scores) {
        switch (type) {
            case D1Q1 -> library.dotProductD1Q1Bulk(dataset, query, documentBytes, count, scores);
            case D1Q4 -> library.dotProductD1Q4Bulk(dataset, query, documentBytes, count, scores);
            case D2Q2 -> library.dotProductD2Q2Bulk(dataset, query, documentBytes, count, scores);
            case D2Q4 -> library.dotProductD2Q4Bulk(dataset, query, documentBytes, count, scores);
            case D4Q4 -> library.dotProductD4Q4Bulk(dataset, query, documentBytes, count, scores);
            case D2Q4_PACKED -> library.dotProductD2Q4PackedBulk(dataset, query, documentBytes, count, scores);
        }
    }
}
