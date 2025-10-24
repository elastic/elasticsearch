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

import org.elasticsearch.nativeaccess.VectorSimilarityFunctionsTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.hamcrest.Matchers.containsString;

public class JDKVectorLibraryFloat32Tests extends VectorSimilarityFunctionsTests {

    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final double delta;

    public JDKVectorLibraryFloat32Tests(int size) {
        super(size);
        this.delta = 1e-5 * size; // scale the delta with the size
    }

    @BeforeClass
    public static void beforeClass() {
        VectorSimilarityFunctionsTests.setup();
    }

    @AfterClass
    public static void afterClass() {
        VectorSimilarityFunctionsTests.cleanup();
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return VectorSimilarityFunctionsTests.parametersFactory();
    }

    public void testAllZeroValues() {
        testFloat32Impl(float[]::new);
    }

    public void testRandomFloats() {
        testFloat32Impl(JDKVectorLibraryFloat32Tests::randomFloatArray);
    }

    public void testFloat32Impl(IntFunction<float[]> vectorGeneratorFunc) {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new float[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs * Float.BYTES);
        for (int i = 0; i < numVecs; i++) {
            values[i] = vectorGeneratorFunc.apply(dims);
            long dstOffset = (long) i * dims * Float.BYTES;
            MemorySegment.copy(MemorySegment.ofArray(values[i]), JAVA_FLOAT_UNALIGNED, 0L, segment, LAYOUT_LE_FLOAT, dstOffset, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            var nativeSeg1 = segment.asSlice((long) first * dims * Float.BYTES, (long) dims * Float.BYTES);
            var nativeSeg2 = segment.asSlice((long) second * dims * Float.BYTES, (long) dims * Float.BYTES);

            // cosine
            float expected = cosineFloat32Scalar(values[first], values[second]);
            assertEquals(expected, cosineFloat32(nativeSeg1, nativeSeg2, dims), delta);
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, cosineFloat32(heapSeg1, heapSeg2, dims), delta);
                assertEquals(expected, cosineFloat32(nativeSeg1, heapSeg2, dims), delta);
                assertEquals(expected, cosineFloat32(heapSeg1, nativeSeg2, dims), delta);
            }

            // dot product
            expected = dotProductFloat32Scalar(values[first], values[second]);
            assertEquals(expected, dotProductFloat32(nativeSeg1, nativeSeg2, dims), delta);
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, dotProductFloat32(heapSeg1, heapSeg2, dims), delta);
                assertEquals(expected, dotProductFloat32(nativeSeg1, heapSeg2, dims), delta);
                assertEquals(expected, dotProductFloat32(heapSeg1, nativeSeg2, dims), delta);
            }

            // square distance
            expected = squareDistanceFloat32Scalar(values[first], values[second]);
            assertEquals(expected, squareDistanceFloat32(nativeSeg1, nativeSeg2, dims), delta);
            if (supportsHeapSegments()) {
                var heapSeg1 = MemorySegment.ofArray(values[first]);
                var heapSeg2 = MemorySegment.ofArray(values[second]);
                assertEquals(expected, squareDistanceFloat32(heapSeg1, heapSeg2, dims), delta);
                assertEquals(expected, squareDistanceFloat32(nativeSeg1, heapSeg2, dims), delta);
                assertEquals(expected, squareDistanceFloat32(heapSeg1, nativeSeg2, dims), delta);
            }

        }
    }

    public void testIllegalDims() {
        assumeTrue(notSupportedMsg(), supported());
        var segment = arena.allocate((long) size * 3 * Float.BYTES);

        var e1 = expectThrows(IAE, () -> cosineFloat32(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e1.getMessage(), containsString("dimensions differ"));
        e1 = expectThrows(IAE, () -> dotProductFloat32(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e1.getMessage(), containsString("dimensions differ"));
        e1 = expectThrows(IAE, () -> squareDistanceFloat32(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e1.getMessage(), containsString("dimensions differ"));

        var e2 = expectThrows(IOOBE, () -> cosineFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));
        e2 = expectThrows(IOOBE, () -> dotProductFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));
        e2 = expectThrows(IOOBE, () -> squareDistanceFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));

        e2 = expectThrows(IOOBE, () -> cosineFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));
        e2 = expectThrows(IOOBE, () -> dotProductFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));
        e2 = expectThrows(IOOBE, () -> squareDistanceFloat32(segment.asSlice(0L, size), segment.asSlice(size, size), -1));
        assertThat(e2.getMessage(), containsString("out of bounds for length"));
    }

    float cosineFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().cosineHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    float dotProductFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().dotProductHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    float squareDistanceFloat32(MemorySegment a, MemorySegment b, int length) {
        try {
            return (float) getVectorDistance().squareDistanceHandleFloat32().invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static float[] randomFloatArray(int length) {
        float[] fa = new float[length];
        for (int i = 0; i < length; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    }

    /** Computes the cosine of the given vectors a and b. */
    static float cosineFloat32Scalar(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double normAA = Math.sqrt(normA);
        double normBB = Math.sqrt(normB);
        if (normAA == 0.0f || normBB == 0.0f) {
            return 0.0f;
        }
        return (float) (dot / (normAA * normBB));
    }

    /** Computes the dot product of the given vectors a and b. */
    static float dotProductFloat32Scalar(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    /** Computes the dot product of the given vectors a and b. */
    static float squareDistanceFloat32Scalar(float[] a, float[] b) {
        float squareSum = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            squareSum += diff * diff;
        }
        return squareSum;
    }
}
