/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

public class BigLongDoubleDoubleArrayTests extends ESTestCase {

    private final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    /** Basic test with trivial small input. If this fails, then all is lost! */
    public void testTrivial() {
        try (LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(3, bigArrays, false)) {

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
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
    public void testSetGet() {
        final int size = randomIntBetween(1, 1_000_000);
        final long startLong = randomIntBetween(1, 1000);
        final double startDouble = randomIntBetween(1, 1000);

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(size, randomBoolean())) {
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
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
    public void testLongDoubleDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);

        LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(startLen, randomBoolean());
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

    public void testIncrement() {
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);

        LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(startLen, randomBoolean());
        long[] longRef = new long[totalLen];
        double[] doubleRef0 = new double[totalLen];
        double[] doubleRef1 = new double[totalLen];
        // initial values
        for (int i = 0; i < totalLen; ++i) {
            longRef[i] = randomLongBetween(1, Long.MAX_VALUE / 2);
            doubleRef0[i] = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            doubleRef1[i] = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            array = bigArrays.grow(array, i + 1);
            array.set(i, longRef[i], doubleRef0[i], doubleRef1[i]);
        }
        // increment
        for (int i = 0; i < totalLen; ++i) {
            long long0Inc = randomLongBetween(1, Long.MAX_VALUE / 2);
            double double0Inc = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            double double1Inc = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            longRef[i] += long0Inc;
            doubleRef0[i] += double0Inc;
            doubleRef1[i] += double1Inc;
            array.increment(i, long0Inc, double0Inc, double1Inc);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(longRef[i], array.getLong0(i));
            assertEquals(doubleRef0[i], array.getDouble0(i), 0.0d);
            assertEquals(doubleRef1[i], array.getDouble1(i), 0.0d);
        }
        array.close();
    }

    /** Tests the estimated ram byte used. For now, always 16K increments, even for small sizes  */
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
    public void testRamBytesUsed() {
        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(1, randomBoolean())) {
            assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k
        }

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(512, randomBoolean())) {
            assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k
        }

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(512 + 1, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(512 + 511, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(512 + 512, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(512 + 512 + 1, randomBoolean())) {
            assertEquals(48 * 1024, array.ramBytesUsed());  // expect 48k (32K + 16k)
        }
    }
}
