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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.DataType;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Function;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions.Operation;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;

/**
 * Tests that bulk-with-offsets scoring works correctly when the vectors
 * MemorySegment is larger than Integer.MAX_VALUE bytes (~2 GB).
 *
 * <p> This is a standalone (non-parameterized) test to avoid running the 2 GB+
 * allocation for every (function, size) combination in the parameterized suites.
 */
public class JDKVectorLibraryLargeSegmentTests extends ESTestCase {

    static {
        NodeNamePatternConverter.setGlobalNodeName("test");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    static boolean supported() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        return jdkVersion >= 21
            && ((arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux")))
                || (arch.equals("amd64") && osName.equals("Linux")));
    }

    static VectorSimilarityFunctions functions;

    @BeforeClass
    public static void setup() {
        assumeTrue("Native vector functions not supported on this platform", supported());
        functions = NativeAccess.instance().getVectorSimilarityFunctions().orElse(null);
        assumeTrue("Vector similarity functions not available", functions != null);
    }

    public void testInt8DotProductBulkWithOffsetsLargeSegment() throws Throwable {
        final int dims = 128;
        final int numVecs = 2;
        long segmentSize = Integer.MAX_VALUE + (long) dims * numVecs;

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment vectorsSegment = arena.allocate(segmentSize);
            byte[][] vectors = new byte[numVecs][];
            for (int i = 0; i < numVecs; i++) {
                vectors[i] = randomByteArrayOfLength(dims);
                MemorySegment.copy(MemorySegment.ofArray(vectors[i]), JAVA_BYTE, 0L, vectorsSegment, JAVA_BYTE, (long) i * dims, dims);
            }

            var offsetsSegment = arena.allocate(Integer.BYTES);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 1);
            var querySegment = vectorsSegment.asSlice(0, dims);
            var scoresSegment = arena.allocate(Float.BYTES);

            functions.getHandle(Function.DOT_PRODUCT, DataType.INT8, Operation.BULK_OFFSETS)
                .invokeExact(vectorsSegment, querySegment, dims, dims, offsetsSegment, 1, scoresSegment);

            float actual = scoresSegment.get(JAVA_FLOAT_UNALIGNED, 0);
            float expected = dotProductI8Scalar(vectors[0], vectors[1]);
            assertEquals(expected, actual, 1e-5f * dims);
        }
    }

    public void testInt8SquareDistanceBulkWithOffsetsLargeSegment() throws Throwable {
        final int dims = 128;
        final int numVecs = 2;
        long segmentSize = Integer.MAX_VALUE + (long) dims * numVecs;

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment vectorsSegment = arena.allocate(segmentSize);
            byte[][] vectors = new byte[numVecs][];
            for (int i = 0; i < numVecs; i++) {
                vectors[i] = randomByteArrayOfLength(dims);
                MemorySegment.copy(MemorySegment.ofArray(vectors[i]), JAVA_BYTE, 0L, vectorsSegment, JAVA_BYTE, (long) i * dims, dims);
            }

            var offsetsSegment = arena.allocate(Integer.BYTES);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 1);
            var querySegment = vectorsSegment.asSlice(0, dims);
            var scoresSegment = arena.allocate(Float.BYTES);

            functions.getHandle(Function.SQUARE_DISTANCE, DataType.INT8, Operation.BULK_OFFSETS)
                .invokeExact(vectorsSegment, querySegment, dims, dims, offsetsSegment, 1, scoresSegment);

            float actual = scoresSegment.get(JAVA_FLOAT_UNALIGNED, 0);
            float expected = squareDistanceI8Scalar(vectors[0], vectors[1]);
            assertEquals(expected, actual, 1e-5f * dims);
        }
    }

    public void testInt8CosineBulkWithOffsetsLargeSegment() throws Throwable {
        final int dims = 128;
        final int numVecs = 2;
        long segmentSize = Integer.MAX_VALUE + (long) dims * numVecs;

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment vectorsSegment = arena.allocate(segmentSize);
            byte[][] vectors = new byte[numVecs][];
            for (int i = 0; i < numVecs; i++) {
                vectors[i] = randomByteArrayOfLength(dims);
                MemorySegment.copy(MemorySegment.ofArray(vectors[i]), JAVA_BYTE, 0L, vectorsSegment, JAVA_BYTE, (long) i * dims, dims);
            }

            var offsetsSegment = arena.allocate(Integer.BYTES);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 1);
            var querySegment = vectorsSegment.asSlice(0, dims);
            var scoresSegment = arena.allocate(Float.BYTES);

            functions.getHandle(Function.COSINE, DataType.INT8, Operation.BULK_OFFSETS)
                .invokeExact(vectorsSegment, querySegment, dims, dims, offsetsSegment, 1, scoresSegment);

            float actual = scoresSegment.get(JAVA_FLOAT_UNALIGNED, 0);
            float expected = cosineI8Scalar(vectors[0], vectors[1]);
            assertEquals(expected, actual, 1e-5f * dims);
        }
    }

    public void testFloat32DotProductBulkWithOffsetsLargeSegment() throws Throwable {
        final int dims = 128;
        final int numVecs = 2;
        long segmentSize = Integer.MAX_VALUE + (long) dims * numVecs * Float.BYTES;

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment vectorsSegment = arena.allocate(segmentSize);
            float[][] vectors = new float[numVecs][];
            for (int i = 0; i < numVecs; i++) {
                vectors[i] = randomFloatArray(dims);
                long dstOffset = (long) i * dims * Float.BYTES;
                MemorySegment.copy(
                    MemorySegment.ofArray(vectors[i]),
                    JAVA_FLOAT_UNALIGNED,
                    0L,
                    vectorsSegment,
                    JAVA_FLOAT_UNALIGNED,
                    dstOffset,
                    dims
                );
            }

            int pitch = dims * Float.BYTES;
            var offsetsSegment = arena.allocate(Integer.BYTES);
            offsetsSegment.setAtIndex(ValueLayout.JAVA_INT, 0, 1);
            var querySegment = vectorsSegment.asSlice(0, (long) dims * Float.BYTES);
            var scoresSegment = arena.allocate(Float.BYTES);

            functions.getHandle(Function.DOT_PRODUCT, DataType.FLOAT32, Operation.BULK_OFFSETS)
                .invokeExact(vectorsSegment, querySegment, dims, pitch, offsetsSegment, 1, scoresSegment);

            float actual = scoresSegment.get(JAVA_FLOAT_UNALIGNED, 0);
            float expected = dotProductF32Scalar(vectors[0], vectors[1]);
            assertEquals(expected, actual, 1e-5f * dims);
        }
    }

    static int dotProductI8Scalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static int squareDistanceI8Scalar(byte[] a, byte[] b) {
        int squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }

    static float cosineI8Scalar(byte[] a, byte[] b) {
        int sum = 0;
        int norm1 = 0;
        int norm2 = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
            norm1 += a[i] * a[i];
            norm2 += b[i] * b[i];
        }
        return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
    }

    static float dotProductF32Scalar(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static float[] randomFloatArray(int length) {
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    }
}
