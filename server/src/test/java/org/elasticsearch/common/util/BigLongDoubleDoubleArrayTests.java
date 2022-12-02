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
        var opaqueIndex = new LongDoubleDoubleArray.OpaqueIndex();
        try (LongDoubleDoubleArray array = new BigLongDoubleDoubleArray(3, bigArrays, false)) {
            opaqueIndex.setForIndex(0);
            array.set(opaqueIndex, 1, 2, 3);
            opaqueIndex.setForIndex(1);
            array.set(opaqueIndex, 9, 8, 7);
            opaqueIndex.setForIndex(2);
            array.set(opaqueIndex, 4, 5, 6);

            opaqueIndex.setForIndex(0);
            assertEquals(1L, array.getLong0(opaqueIndex));
            assertEquals(2d, array.getDouble0(opaqueIndex), 0.0d);
            assertEquals(3d, array.getDouble1(opaqueIndex), 0.0d);

            opaqueIndex.setForIndex(1);
            assertEquals(9L, array.getLong0(opaqueIndex));
            assertEquals(8d, array.getDouble0(opaqueIndex), 0.0d);
            assertEquals(7d, array.getDouble1(opaqueIndex), 0.0d);

            opaqueIndex.setForIndex(2);
            assertEquals(4L, array.getLong0(opaqueIndex));
            assertEquals(5d, array.getDouble0(opaqueIndex), 0.0d);
            assertEquals(6d, array.getDouble1(opaqueIndex), 0.0d);
        }
    }

    @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public void testSetGet() {
        final int size = randomIntBetween(1, 1_000_000);
        final long startLong = randomIntBetween(1, 1000);
        final double startDouble = randomIntBetween(1, 1000);
        var opaqueIndex = new LongDoubleDoubleArray.OpaqueIndex();

        try (LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(size, randomBoolean())) {
            long longValue = startLong;
            double doubleValue = startDouble;
            for (int i = 0; i < size; i++) {
                opaqueIndex.setForIndex(i);
                array.set(opaqueIndex, longValue++, doubleValue++, doubleValue++);
            }

            for (int i = size - 1; i >= 0; i--) {
                opaqueIndex.setForIndex(i);
                assertEquals(--doubleValue, array.getDouble1(opaqueIndex), 0.0d);
                assertEquals(--doubleValue, array.getDouble0(opaqueIndex), 0.0d);
                assertEquals(--longValue, array.getLong0(opaqueIndex));
            }
            assertEquals(startLong, longValue);
            assertEquals(startDouble, doubleValue, 0.0d);
        }
    }

    @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public void testLongDoubleDoubleArrayGrowth() {
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);

        LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(startLen, randomBoolean());
        long[] longRef = new long[totalLen];
        double[] doubleRef0 = new double[totalLen];
        double[] doubleRef1 = new double[totalLen];
        var holder = new LongDoubleDoubleArray.Holder();
        var opaqueIndex = new LongDoubleDoubleArray.OpaqueIndex();
        for (int i = 0; i < totalLen; ++i) {
            longRef[i] = randomLong();
            doubleRef0[i] = randomDouble();
            doubleRef1[i] = randomDouble();
            array = bigArrays.grow(array, i + 1);
            opaqueIndex.setForIndex(i);
            array.set(opaqueIndex, longRef[i], doubleRef0[i], doubleRef1[i]);
        }
        for (int i = 0; i < totalLen; ++i) {
            opaqueIndex.setForIndex(i);
            assertEquals(longRef[i], array.getLong0(opaqueIndex));
            assertEquals(doubleRef0[i], array.getDouble0(opaqueIndex), 0.0d);
            assertEquals(doubleRef1[i], array.getDouble1(opaqueIndex), 0.0d);
            array.get(opaqueIndex, holder);
            assertEquals(longRef[i], holder.getLong0());
            assertEquals(doubleRef0[i], holder.getDouble0(), 0.0d);
            assertEquals(doubleRef1[i], holder.getDouble1(), 0.0d);
        }
        array.close();
    }

    @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public void testIncrement() {
        final int totalLen = randomIntBetween(1, 1_000_000);
        final int startLen = randomIntBetween(1, randomBoolean() ? 1000 : totalLen);

        LongDoubleDoubleArray array = bigArrays.newLongDoubleDoubleArray(startLen, randomBoolean());
        long[] longRef = new long[totalLen];
        double[] doubleRef0 = new double[totalLen];
        double[] doubleRef1 = new double[totalLen];
        var opaqueIndex = new LongDoubleDoubleArray.OpaqueIndex();
        // initial values
        for (int i = 0; i < totalLen; ++i) {
            longRef[i] = randomLongBetween(1, Long.MAX_VALUE / 2);
            doubleRef0[i] = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            doubleRef1[i] = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            array = bigArrays.grow(array, i + 1);
            opaqueIndex.setForIndex(i);
            array.set(opaqueIndex, longRef[i], doubleRef0[i], doubleRef1[i]);
        }
        // increment
        for (int i = 0; i < totalLen; ++i) {
            long long0Inc = randomLongBetween(1, Long.MAX_VALUE / 2);
            double double0Inc = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            double double1Inc = randomDoubleBetween(1, Double.MAX_VALUE / 2, true);
            longRef[i] += long0Inc;
            doubleRef0[i] += double0Inc;
            doubleRef1[i] += double1Inc;
            opaqueIndex.setForIndex(i);
            array.increment(opaqueIndex, long0Inc, double0Inc, double1Inc);
        }
        for (int i = 0; i < totalLen; ++i) {
            opaqueIndex.setForIndex(i);
            assertEquals(longRef[i], array.getLong0(opaqueIndex));
            assertEquals(doubleRef0[i], array.getDouble0(opaqueIndex), 0.0d);
            assertEquals(doubleRef1[i], array.getDouble1(opaqueIndex), 0.0d);
        }
        array.close();
    }

    /** Tests the estimated ram byte used. For now, always 16K increments, even for small sizes  */
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 1000)
    /*
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
    */
}
