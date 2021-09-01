/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assume.assumeThat;

public class BitArrayTests extends ESTestCase {

    public void testRandom() {
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            int numBits = randomIntBetween(1000, 10000);
            for (int step = 0; step < 3; step++) {
                boolean[] bits = new boolean[numBits];
                List<Integer> slots = new ArrayList<>();
                for (int i = 0; i < numBits; i++) {
                    bits[i] = randomBoolean();
                    slots.add(i);
                }
                Collections.shuffle(slots, random());
                for (int i : slots) {
                    if (bits[i]) {
                        bitArray.set(i);
                    } else {
                        bitArray.clear(i);
                    }
                }
                for (int i = 0; i < numBits; i++) {
                    assertEquals(bitArray.get(i), bits[i]);
                }
            }
        }
    }

    public void testVeryLarge() {
        assumeThat(Runtime.getRuntime().maxMemory(), greaterThanOrEqualTo(ByteSizeUnit.MB.toBytes(512)));
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            long index = randomLongBetween(Integer.MAX_VALUE, (long) (Integer.MAX_VALUE * 1.5));
            assertFalse(bitArray.get(index));
            bitArray.set(index);
            assertTrue(bitArray.get(index));
            bitArray.clear(index);
            assertFalse(bitArray.get(index));
        }
    }

    public void testTooBigIsNotSet() {
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            for (int i = 0; i < 1000; i++) {
                /*
                 * The first few times this is called we check within the
                 * array. But we quickly go beyond it and those all return
                 * false as well.
                 */
                assertFalse(bitArray.get(i));
            }
        }
    }

    public void testClearingDoesntAllocate() {
        ByteSizeValue max = new ByteSizeValue(1, ByteSizeUnit.KB);
        MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), max);
        try (BitArray bitArray = new BitArray(1, bigArrays)) {
            bitArray.clear(100000000);
        }
    }

    public void testAllocation() {
        MockBigArrays.assertFitsIn(new ByteSizeValue(100), bigArrays -> new BitArray(1, bigArrays));
    }

    public void testOr() {
        try (BitArray bitArray1 = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE);
             BitArray bitArray2 = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE);
             BitArray bitArrayFull = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            int numBits = randomIntBetween(1000, 10000);
            for (int step = 0; step < 3; step++) {
                for (int i = 0; i < numBits; i++) {
                    if (randomBoolean()) {
                        if (rarely()) {
                            bitArray1.set(i);
                            bitArray2.set(i);
                        } else if (randomBoolean()) {
                            bitArray1.set(i);
                        } else {
                            bitArray2.set(i);
                        }
                        bitArrayFull.set(i);
                    }
                }
                bitArray1.or(bitArray2);
                for (int i = 0; i < numBits; i++) {
                    assertEquals(bitArrayFull.get(i), bitArray1.get(i));
                }
            }
        }
    }

    public void testNextBitSet() {
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            int numBits = randomIntBetween(1000, 10000);
            for (int step = 0; step < 3; step++) {
                for (int i = 0; i < numBits; i++) {
                    if (randomBoolean()) {
                        bitArray.set(i);
                    }
                }
                long next = bitArray.nextSetBit(0);
                for (int i = 0; i < numBits; i++) {
                    if (i == next) {
                        assertEquals(true, bitArray.get(i));
                        if (i < numBits - 1) {
                            next = bitArray.nextSetBit(i + 1);
                        }
                    } else {
                        assertEquals(false, bitArray.get(i));
                    }
                }
            }
        }
    }

    public void testCardinality() {
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            int numBits = randomIntBetween(1000, 10000);
            long cardinality = 0;
            for (int step = 0; step < 3; step++) {
                for (int i = 0; i < numBits; i++) {
                    if (randomBoolean()) {
                        if (bitArray.get(i) == false) {
                            cardinality++;
                        }
                        bitArray.set(i);
                    }
                }
                assertEquals(cardinality, bitArray.cardinality());
            }
        }
    }
}
