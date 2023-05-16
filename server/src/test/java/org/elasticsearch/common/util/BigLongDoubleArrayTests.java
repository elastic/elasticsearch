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

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
public class BigLongDoubleArrayTests extends ESTestCase {

    private final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    /** Basic test with trivial small input. If this fails, then all is lost! */
    public void testTrivial() {
        try (LongDoubleArray array = bigArrays.newLongDoubleArray(3, false)) {
            array.set(0, 1, 2);
            array.set(1, 9, 8);
            array.set(2, 4, 5);

            assertEquals(1L, array.getLong(0));
            assertEquals(2d, array.getDouble(0), 0.0d);

            assertEquals(9L, array.getLong(1));
            assertEquals(8d, array.getDouble(1), 0.0d);

            assertEquals(4L, array.getLong(2));
            assertEquals(5d, array.getDouble(2), 0.0d);
        }
    }

    public void testSetGet() {
        final int size = randomIntBetween(1, 1_000_000);
        final long startLong = randomIntBetween(1, 1000);
        final double startDouble = randomIntBetween(1, 1000);

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(size, randomBoolean())) {
            long longValue = startLong;
            double doubleValue = startDouble;
            for (int i = 0; i < size; i++) {
                array.set(i, longValue++, doubleValue++);
            }

            int count = 0;
            for (int i = size - 1; i >= 0; i--) {
                assertEquals(--doubleValue, array.getDouble(i), 0.0d);
                assertEquals(--longValue, array.getLong(i));
            }
            assertEquals(startLong, longValue);
            assertEquals(startDouble, doubleValue, 0.0d);
        }
    }

    public void testLongDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);

        LongDoubleArray array = bigArrays.newLongDoubleArray(startLen, randomBoolean());
        long[] longRef = new long[totalLen];
        double[] doubleRef0 = new double[totalLen];
        double[] doubleRef1 = new double[totalLen];
        for (int i = 0; i < totalLen; ++i) {
            longRef[i] = randomLong();
            doubleRef0[i] = randomDouble();
            doubleRef1[i] = randomDouble();
            array = bigArrays.grow(array, i + 1);
            array.set(i, longRef[i], doubleRef0[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            assertEquals(longRef[i], array.getLong(i));
            assertEquals(doubleRef0[i], array.getDouble(i), 0.0d);
        }
        array.close();
    }

    /** Tests the estimated ram byte used. For now, always 16K increments, even for small sizes  */
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
    /*
    public void testRamBytesUsed() {
        try (LongDoubleArray array = bigArrays.newLongDoubleArray(1, randomBoolean())) {
            assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k
        }

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(512, randomBoolean())) {
            assertEquals(1 << 14, array.ramBytesUsed());  // expect 16k
        }

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(512 + 1, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(512 + 511, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(512 + 512, randomBoolean())) {
            assertEquals(1 << 15, array.ramBytesUsed());  // expect 32k
        }

        try (LongDoubleArray array = bigArrays.newLongDoubleArray(512 + 512 + 1, randomBoolean())) {
            assertEquals(48 * 1024, array.ramBytesUsed());  // expect 48k (32K + 16k)
        }
    }
    */
}
