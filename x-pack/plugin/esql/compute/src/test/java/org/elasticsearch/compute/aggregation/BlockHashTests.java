/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

public class BlockHashTests extends ESTestCase {

    public void testBasicIntHash() {
        int[] values = new int[] { 1, 2, 3, 1, 2, 3, 1, 2, 3 };
        IntBlock block = new IntArrayVector(values, values.length, null).asBlock();

        IntBlock keysBlock;
        try (
            BlockHash hashBlock = BlockHash.newHashForType(
                BlockHash.Type.INT,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService())
            )
        ) {
            assertEquals(0, hashBlock.add(block, 0));
            assertEquals(1, hashBlock.add(block, 1));
            assertEquals(2, hashBlock.add(block, 2));
            assertEquals(-1, hashBlock.add(block, 3));
            assertEquals(-2, hashBlock.add(block, 4));
            assertEquals(-3, hashBlock.add(block, 5));
            assertEquals(-1, hashBlock.add(block, 6));
            assertEquals(-2, hashBlock.add(block, 7));
            assertEquals(-3, hashBlock.add(block, 8));
            keysBlock = (IntBlock) hashBlock.getKeys();
        }

        long[] expectedKeys = new long[] { 1, 2, 3 };
        assertEquals(expectedKeys.length, keysBlock.getPositionCount());
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals(expectedKeys[i], keysBlock.getInt(i));
        }
    }

    public void testBasicLongHash() {
        long[] values = new long[] { 2, 1, 4, 2, 4, 1, 3, 4 };
        LongBlock block = new LongArrayVector(values, values.length).asBlock();

        LongBlock keysBlock;
        try (
            BlockHash longHash = BlockHash.newHashForType(
                BlockHash.Type.LONG,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService())
            )
        ) {
            assertEquals(0, longHash.add(block, 0));
            assertEquals(1, longHash.add(block, 1));
            assertEquals(2, longHash.add(block, 2));
            assertEquals(-1, longHash.add(block, 3));
            assertEquals(-3, longHash.add(block, 4));
            assertEquals(-2, longHash.add(block, 5));
            assertEquals(3, longHash.add(block, 6));
            assertEquals(-3, longHash.add(block, 7));
            keysBlock = (LongBlock) longHash.getKeys();
        }

        long[] expectedKeys = new long[] { 2, 1, 4, 3 };
        assertEquals(expectedKeys.length, keysBlock.getPositionCount());
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals(expectedKeys[i], keysBlock.getLong(i));
        }
    }

    public void testBasicLongDouble() {
        double[] values = new double[] { 2.0, 1.0, 4.0, 2.0, 4.0, 1.0, 3.0, 4.0 };
        DoubleBlock block = new DoubleArrayVector(values, values.length).asBlock();

        DoubleBlock keysBlock;
        try (
            BlockHash longHash = BlockHash.newHashForType(
                BlockHash.Type.DOUBLE,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService())
            )
        ) {
            assertEquals(0, longHash.add(block, 0));
            assertEquals(1, longHash.add(block, 1));
            assertEquals(2, longHash.add(block, 2));
            assertEquals(-1, longHash.add(block, 3));
            assertEquals(-3, longHash.add(block, 4));
            assertEquals(-2, longHash.add(block, 5));
            assertEquals(3, longHash.add(block, 6));
            assertEquals(-3, longHash.add(block, 7));
            keysBlock = (DoubleBlock) longHash.getKeys();
        }

        double[] expectedKeys = new double[] { 2.0, 1.0, 4.0, 3.0 };
        assertEquals(expectedKeys.length, keysBlock.getPositionCount());
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals(expectedKeys[i], keysBlock.getDouble(i), 0.0);
        }
    }

    @SuppressWarnings("unchecked")
    public void testBasicBytesRefHash() {
        var builder = BytesRefBlock.newBlockBuilder(8);
        builder.appendBytesRef(new BytesRef("item-2"));
        builder.appendBytesRef(new BytesRef("item-1"));
        builder.appendBytesRef(new BytesRef("item-4"));
        builder.appendBytesRef(new BytesRef("item-2"));
        builder.appendBytesRef(new BytesRef("item-4"));
        builder.appendBytesRef(new BytesRef("item-1"));
        builder.appendBytesRef(new BytesRef("item-3"));
        builder.appendBytesRef(new BytesRef("item-4"));
        BytesRefBlock block = builder.build();

        BytesRefBlock keysBlock;
        try (
            BlockHash longHash = BlockHash.newHashForType(
                BlockHash.Type.BYTES_REF,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService())
            )
        ) {
            assertEquals(0, longHash.add(block, 0));
            assertEquals(1, longHash.add(block, 1));
            assertEquals(2, longHash.add(block, 2));
            assertEquals(-1, longHash.add(block, 3));
            assertEquals(-3, longHash.add(block, 4));
            assertEquals(-2, longHash.add(block, 5));
            assertEquals(3, longHash.add(block, 6));
            assertEquals(-3, longHash.add(block, 7));
            keysBlock = (BytesRefBlock) longHash.getKeys();
        }

        BytesRef[] expectedKeys = new BytesRef[] {
            new BytesRef("item-2"),
            new BytesRef("item-1"),
            new BytesRef("item-4"),
            new BytesRef("item-3") };
        assertEquals(expectedKeys.length, keysBlock.getPositionCount());
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals(expectedKeys[i], keysBlock.getBytesRef(i, new BytesRef()));
        }
    }
}
