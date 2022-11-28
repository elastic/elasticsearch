/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

public class BigLongDoubleDoubleArrayTests extends ESTestCase {

    /** Basic test with trivial small input. If this fails, then all is lost! */
    public void testTrivial() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(3, bigArrays, false);

        array.set(0, 1, 2, 3);
        array.set(1, 9, 8, 7);
        array.set(2, 4, 5, 6);

        assertEquals(1L, array.getLong0(0));
        assertEquals(2d, array.getDouble0(0), 0.0d);
        assertEquals(3d, array.getDouble1(0), 0.0d);

        assertEquals(9L, array.getLong0(1));
        assertEquals(8d, array.getDouble0(1), 0.0d);
        assertEquals(7d, array.getDouble1(1), 0.0d);

        assertEquals(4L, array.getLong0(2));
        assertEquals(5d, array.getDouble0(2), 0.0d);
        assertEquals(6d, array.getDouble1(2), 0.0d);
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testSetGet() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        final int size = randomIntBetween(1, 1_000_000);
        final long startLong = randomIntBetween(1, 1000);
        final double startDouble = randomIntBetween(1, 1000);

        LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(size, bigArrays, false);
        long longValue = startLong;
        double doubleValue = startDouble;
        for (int i = 0; i < size; i++) {
            array.set(i, longValue++, doubleValue++, doubleValue++);
        }

        for (int i = size - 1; i >= 0; i--) {
            assertEquals(--doubleValue, array.getDouble1(i), 0.0d);
            assertEquals(--doubleValue, array.getDouble0(i), 0.0d);
            assertEquals(--longValue, array.getLong0(i));
        }
        assertEquals(startLong, longValue);
        assertEquals(startDouble, doubleValue, 0.0d);
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10000)
    public void testLongArrayGrowth() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);
        LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(startLen, bigArrays, randomBoolean());
        long[] longRef = new long[totalLen];
        double[] doubleRef0 = new double[totalLen];
        double[] doubleRef1 = new double[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            longRef[i] = randomLong();
            doubleRef0[i] = randomDouble();
            doubleRef1[i] = randomDouble();
            array = bigArrays.grow(array, i + 1);
            array.set(i, longRef[i], doubleRef0[i], doubleRef1[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(longRef[i], array.getLong0(i));
            assertEquals(doubleRef0[i], array.getDouble0(i), 0.0d);
            assertEquals(doubleRef1[i], array.getDouble1(i), 0.0d);
        }
        array.close();
    }

    /** Tests the estimated ram byte used. For now, always 16K increments, even for small sizes  */
    public void testRamBytesUsed() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(1, bigArrays, false);
        assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k

        array = new BigLongDoubleDoubleArray(512, bigArrays, false);
        assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k

        array = new BigLongDoubleDoubleArray(512 + 1, bigArrays, false);
        assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k

        array = new BigLongDoubleDoubleArray(512 + 511, bigArrays, false);
        assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k

        array = new BigLongDoubleDoubleArray(512 + 512, bigArrays, false);
        assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k

        array = new BigLongDoubleDoubleArray(512 + 512 + 1, bigArrays, false);
        assertEquals(48 * 1024, array.ramBytesUsed());  // expect 48k (32K + 16k)
    }
}
