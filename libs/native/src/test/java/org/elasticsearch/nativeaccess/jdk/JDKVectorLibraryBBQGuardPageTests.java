/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;
import org.elasticsearch.nativeaccess.BBQTestUtils;
import org.elasticsearch.nativeaccess.GuardPageAllocator;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function.DOT_PRODUCT;

/**
 * Tests that BBQ native vector kernels do not over-read their input buffers.
 *
 * <p>Each vector (doc and query) is allocated so that its last byte is the final
 * byte of a memory-mapped page. The virtual address immediately after is unmapped,
 * so any native read past the buffer boundary will deliver SIGBUS/SIGSEGV rather
 * than silently returning wrong results.
 *
 * <p>This is a regression test for the over-read bug in {@code dotd1qN_inner_bulk}'s
 * byte-tail loop, which performed 8-byte loads while advancing 1 byte per step.
 * The bug was fixed by switching to single-byte loads in the tail.
 *
 * <p>Runs on Linux and macOS, on both aarch64 and amd64.
 */
public class JDKVectorLibraryBBQGuardPageTests extends ESTestCase {

    static {
        NodeNamePatternConverter.setGlobalNodeName("guard-page-test");
        LogConfigurator.configureESLogging();
    }

    private static VectorSimilarityFunctions functions;
    private static int pageSize;

    @BeforeClass
    public static void beforeClass() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        assumeTrue(
            "Requires aarch64 or amd64 on Linux or macOS with JDK >= 21",
            jdkVersion >= 21
                && ((arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux")))
                    || (arch.equals("amd64") && osName.equals("Linux")))
        );
        var vsf = NativeAccess.instance().getVectorSimilarityFunctions();
        assumeTrue("Vector similarity functions not available", vsf.isPresent());
        functions = vsf.get();

        pageSize = NativeLibraryProvider.instance().getLibrary(PosixCLibrary.class).getPageSize();
    }

    @AfterClass
    public static void afterClass() {
        functions = null;
    }

    public void testSinglePairD1Q4TailLengths() throws IOException {
        // D1Q4: 1-bit data, 4-bit query (striped). indexBytes = dims/8, queryBytes = 4*indexBytes.
        // Byte-tail is exercised when indexBytes % 16 != 0 (chunk_size = 16 on NEON/SSE).
        // We test all tail lengths 1..15 and a few that include full chunks + tail.
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 19, 23, 31, 33 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 8;

                byte[] unpackedDoc = new byte[dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedDoc, (byte) 0, (byte) 1);
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 15);

                byte[] packedDoc = BBQTestUtils.packStriped(unpackedDoc, 1);
                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);
                assert packedDoc.length == indexBytes;

                MemorySegment docSeg = arena.allocateAtPageEnd(packedDoc);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                long actual = functions.dotProductD1Q4(docSeg, querySeg, indexBytes);
                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("D1Q4 single-pair failed for indexBytes=" + indexBytes, expected, (float) actual, 0f);
            }
        }
    }

    public void testBulkD1Q4TailLengths() throws IOException {
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 1, 3, 5, 7, 9, 11, 13, 15, 17, 23, 31 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 8;
                int numVecs = randomIntBetween(2, 8);

                byte[][] unpackedDocs = new byte[numVecs][dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 15);

                byte[][] packedDocs = new byte[numVecs][];
                byte[] contiguousDocs = new byte[numVecs * indexBytes];
                for (int i = 0; i < numVecs; i++) {
                    randomBytesBetween(unpackedDocs[i], (byte) 0, (byte) 1);
                    packedDocs[i] = BBQTestUtils.packStriped(unpackedDocs[i], 1);
                    System.arraycopy(packedDocs[i], 0, contiguousDocs, i * indexBytes, indexBytes);
                }

                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);

                MemorySegment docsSeg = arena.allocateAtPageEnd(contiguousDocs);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);
                MemorySegment scoresSeg = arena.allocate((long) numVecs * Float.BYTES);

                functions.dotProductD1Q4Bulk(docsSeg, querySeg, indexBytes, numVecs, scoresSeg);

                for (int i = 0; i < numVecs; i++) {
                    float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDocs[i]);
                    float actual = scoresSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES);
                    assertEquals("D1Q4 bulk failed for indexBytes=" + indexBytes + ", vec=" + i, expected, actual, 0f);
                }
            }
        }
    }

    public void testBulkSparseD1Q4TailLengths() throws IOException {
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 19, 23, 31, 33 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 8;
                int numVecs = randomIntBetween(2, 8);

                byte[][] unpackedDocs = new byte[numVecs][dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 15);

                // Each doc vector in its own guard-page-backed segment
                MemorySegment[] docSegments = new MemorySegment[numVecs];
                for (int i = 0; i < numVecs; i++) {
                    randomBytesBetween(unpackedDocs[i], (byte) 0, (byte) 1);
                    byte[] packedDoc = BBQTestUtils.packStriped(unpackedDocs[i], 1);
                    docSegments[i] = arena.allocateAtPageEnd(packedDoc);
                }

                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                // Build address table
                MemorySegment addressesSeg = arena.allocate(
                    ValueLayout.ADDRESS.byteSize() * numVecs,
                    ValueLayout.ADDRESS.byteAlignment()
                );
                for (int i = 0; i < numVecs; i++) {
                    addressesSeg.setAtIndex(ValueLayout.ADDRESS, i, docSegments[i]);
                }

                MemorySegment scoresSeg = arena.allocate((long) numVecs * Float.BYTES);
                functions.dotProductD1Q4BulkSparse(addressesSeg, querySeg, indexBytes, numVecs, scoresSeg);

                for (int i = 0; i < numVecs; i++) {
                    float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDocs[i]);
                    float actual = scoresSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES);
                    assertEquals("D1Q4 bulk-sparse failed for indexBytes=" + indexBytes + ", vec=" + i, expected, actual, 0f);
                }
            }
        }
    }

    public void testSinglePairD1Q1TailLengths() throws IOException {
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 31 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 8;

                byte[] unpackedDoc = new byte[dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedDoc, (byte) 0, (byte) 1);
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 1);

                byte[] packedDoc = BBQTestUtils.packStriped(unpackedDoc, 1);
                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 1);

                MemorySegment docSeg = arena.allocateAtPageEnd(packedDoc);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                long actual = functions.dotProductD1Q1(docSeg, querySeg, indexBytes);
                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("D1Q1 single-pair failed for indexBytes=" + indexBytes, expected, (float) actual, 0f);
            }
        }
    }

    public void testBulkD1Q1TailLengths() throws IOException {
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 1, 3, 5, 7, 9, 11, 13, 15, 17, 23 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 8;
                int numVecs = randomIntBetween(2, 8);

                byte[][] unpackedDocs = new byte[numVecs][dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 1);

                byte[] contiguousDocs = new byte[numVecs * indexBytes];
                for (int i = 0; i < numVecs; i++) {
                    randomBytesBetween(unpackedDocs[i], (byte) 0, (byte) 1);
                    byte[] packed = BBQTestUtils.packStriped(unpackedDocs[i], 1);
                    System.arraycopy(packed, 0, contiguousDocs, i * indexBytes, indexBytes);
                }

                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 1);

                MemorySegment docsSeg = arena.allocateAtPageEnd(contiguousDocs);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);
                MemorySegment scoresSeg = arena.allocate((long) numVecs * Float.BYTES);

                functions.dotProductD1Q1Bulk(docsSeg, querySeg, indexBytes, numVecs, scoresSeg);

                for (int i = 0; i < numVecs; i++) {
                    float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDocs[i]);
                    float actual = scoresSeg.get(ValueLayout.JAVA_FLOAT_UNALIGNED, (long) i * Float.BYTES);
                    assertEquals("D1Q1 bulk failed for indexBytes=" + indexBytes + ", vec=" + i, expected, actual, 0f);
                }
            }
        }
    }

    public void testSinglePairD2Q2TailLengths() throws IOException {
        // D2Q2: 2-bit data, 2-bit query. indexBytes = dims*2/8 = dims/4.
        // The kernel splits length in half for lower/upper, so tail triggered by (indexBytes/2) % 16 != 0.
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 2, 4, 6, 8, 10, 14, 18, 22, 30 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 4; // 2 bits per dim => dims/4 bytes per plane, 2 planes

                byte[] unpackedDoc = new byte[dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedDoc, (byte) 0, (byte) 3);
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 3);

                byte[] packedDoc = BBQTestUtils.packStriped(unpackedDoc, 2);
                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 2);
                assert packedDoc.length == indexBytes;
                assert packedQuery.length == indexBytes;

                MemorySegment docSeg = arena.allocateAtPageEnd(packedDoc);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                long actual = functions.dotProductD2Q2(docSeg, querySeg, indexBytes);
                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("D2Q2 single-pair failed for indexBytes=" + indexBytes, expected, (float) actual, 0f);
            }
        }
    }

    public void testSinglePairD2Q4TailLengths() throws IOException {
        // D2Q4: 2-bit data, 4-bit query. indexBytes = dims*2/8 = dims/4. queryBytes = dims*4/8 = dims/2.
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 2, 4, 6, 8, 10, 14, 18, 22, 30 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 4;

                byte[] unpackedDoc = new byte[dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedDoc, (byte) 0, (byte) 3);
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 15);

                byte[] packedDoc = BBQTestUtils.packStriped(unpackedDoc, 2);
                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);
                assert packedDoc.length == indexBytes;

                MemorySegment docSeg = arena.allocateAtPageEnd(packedDoc);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                long actual = functions.dotProductD2Q4(docSeg, querySeg, indexBytes);
                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("D2Q4 single-pair failed for indexBytes=" + indexBytes, expected, (float) actual, 0f);
            }
        }
    }

    public void testSinglePairD4Q4TailLengths() throws IOException {
        // D4Q4: 4-bit data, 4-bit query. indexBytes = dims*4/8 = dims/2. queryBytes = dims/2.
        // The kernel splits into 4 sub-vectors, so tail triggered by (indexBytes/4) % 16 != 0.
        try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
            int[] byteLengths = { 4, 8, 12, 16, 20, 28, 36, 44, 60 };
            for (int indexBytes : byteLengths) {
                int dims = indexBytes * 2; // 4 bits per dim

                byte[] unpackedDoc = new byte[dims];
                byte[] unpackedQuery = new byte[dims];
                randomBytesBetween(unpackedDoc, (byte) 0, (byte) 15);
                randomBytesBetween(unpackedQuery, (byte) 0, (byte) 15);

                byte[] packedDoc = BBQTestUtils.packStriped(unpackedDoc, 4);
                byte[] packedQuery = BBQTestUtils.packStriped(unpackedQuery, 4);
                assert packedDoc.length == indexBytes;
                assert packedQuery.length == indexBytes;

                MemorySegment docSeg = arena.allocateAtPageEnd(packedDoc);
                MemorySegment querySeg = arena.allocateAtPageEnd(packedQuery);

                long actual = functions.dotProductD4Q4(docSeg, querySeg, indexBytes);
                float expected = ScalarOperations.similarity(DOT_PRODUCT, unpackedQuery, unpackedDoc);
                assertEquals("D4Q4 single-pair failed for indexBytes=" + indexBytes, expected, (float) actual, 0f);
            }
        }
    }
}
