/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.nativeaccess.AbstractVectorTestCase;
import org.junit.AfterClass;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class NativeAccessTests extends AbstractVectorTestCase {

    static final int[] VECTOR_DIMS = { 1, 4, 6, 8, 13, 16, 25, 31, 32, 33, 64, 100, 128, 207, 256, 300, 512, 702, 1023, 1024, 1025 };

    final int size;

    final VectorDistance impl;
    static Arena arena;

    public NativeAccessTests(int size) {
        this.size = size;
        impl = VectorDistanceProvider.lookup();
        arena = Arena.ofConfined();
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> IntStream.of(VECTOR_DIMS).boxed().map(i -> new Object[] { i }).iterator();
    }

    public void testBinaryVectors() {
        final int dims = size;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][dims];
        var segment = arena.allocate((long) dims * numVecs);
        for (int i = 0; i < numVecs; i++) {
            random().nextBytes(values[i]);
            MemorySegment.copy(MemorySegment.ofArray(values[i]), 0L, segment, (long) i * dims, dims);
        }

        final int loopTimes = 1000;
        for (int i = 0; i < loopTimes; i++) {
            int first = randomInt(numVecs - 1);
            int second = randomInt(numVecs - 1);
            int implDot = impl.dotProduct(segment.asSlice((long) first * dims, dims), segment.asSlice((long) second * dims, dims), dims);
            int otherDot = dotProductScalar(values[first], values[second]);
            assertEquals(otherDot, implDot);
        }
        // assertIntReturningProviders(p -> p.squareDistance(a, b));
    }

    public void testIllegalDims() {
        var segment = arena.allocate((long) size * 3);
        var e = expectThrows(IAE, () -> impl.dotProduct(segment.asSlice(0L, size), segment.asSlice(size, size + 1), size));
        assertThat(e.getMessage(), containsString("dimensions differ"));

        e = expectThrows(IAE, () -> impl.dotProduct(segment.asSlice(0L, size), segment.asSlice(size, size), size + 1));
        assertThat(e.getMessage(), containsString("greater than vector dimensions"));
    }

    @AfterClass
    public static void cleanup() {
        arena.close();
    }

    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;
}
