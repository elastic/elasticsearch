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

public class JDKVectorLibraryInt4BitTests extends VectorSimilarityFunctionsTests {

    public JDKVectorLibraryInt4BitTests(int size) {
        super(size);
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

    private static int discretize(int value, int bucket) {
        return ((value + (bucket - 1)) / bucket) * bucket;
    }

    public void testInt4Bin() {
        assumeTrue(notSupportedMsg(), supported());
        final int length = discretize(size, 64) / 8;
        final int numVecs = randomIntBetween(2, 101);
        var values = new byte[numVecs][length];
        var segment = arena.allocate((long) numVecs * length);
        for (int i = 0; i < numVecs; i++) {
            random().nextBytes(values[i]);
            MemorySegment.copy(MemorySegment.ofArray(values[i]), 0L, segment, (long) i * length, length);
        }

        final int loopTimes = 1000;
        byte[] query = new byte[4 * length];
        float[] scores = new float[numVecs];
        float[] scoresExpected = new float[numVecs];
        var querySegment = arena.allocate(4L * length);
        for (int i = 0; i < loopTimes; i++) {
            int ord = randomInt(numVecs - 1);
            long offset = (long) ord * length;
            random().nextBytes(query);
            MemorySegment.copy(MemorySegment.ofArray(query), 0L, querySegment, 0, 4 * length);
            for (int j = 0; j < numVecs; j++) {
                scoresExpected[j] = int4BitScalar(query, values[j], length);
            }
            assertEquals(scoresExpected[ord], (float) int4Bit(querySegment, segment, offset, length), 0.0f);
            int4BitBulk(querySegment, segment, 0L, MemorySegment.ofArray(scores), numVecs, length);
            assertArrayEquals(scoresExpected, scores, 0.0f);
        }
    }

    long int4Bit(MemorySegment a, MemorySegment b, long offset, int length) {
        try {
            return (long) getVectorDistance().int4BitDotProductHandle().invokeExact(a, b, offset, length);
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

    void int4BitBulk(MemorySegment a, MemorySegment b, long offset, MemorySegment scores, int count, int length) {
        try {
            getVectorDistance().int4BitDotProductBulkHandle().invokeExact(a, b, offset, scores, count, length);
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

    /** Computes the dot product of the given vectors a and b. */
    static long int4BitScalar(byte[] a, byte[] b, int length) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        for (int r = 0; r < length; r++) {
            final byte value = b[r];
            subRet0 += Integer.bitCount((a[r] & value) & 0xFF);
            subRet1 += Integer.bitCount((a[r + length] & value) & 0xFF);
            subRet2 += Integer.bitCount((a[r + 2 * length] & value) & 0xFF);
            subRet3 += Integer.bitCount((a[r + 3 * length] & value) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }
}
