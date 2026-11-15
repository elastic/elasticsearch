/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.BitSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LocalBlockPoolTests extends ComputeTestCase {
    private static final int MIN_LONG_LENGTH = Math.toIntExact(LocalBlockPool.MIN_ARRAY_BYTES / Long.BYTES);
    private static final int MIN_INT_LENGTH = Math.toIntExact(LocalBlockPool.MIN_ARRAY_BYTES / Integer.BYTES);
    private static final int MAX_LONG_LENGTH = Math.toIntExact(LocalBlockPool.MAX_ARRAY_BYTES / Long.BYTES);
    private static final int MAX_INT_LENGTH = Math.toIntExact(LocalBlockPool.MAX_ARRAY_BYTES / Integer.BYTES);

    private static void recycle(LocalCircuitBreaker local, long[] arr) {
        local.addEstimateBytesAndMaybeBreak(LocalBlockPool.usedBytesForLongs(arr.length), "test");
        local.pool().recycleLongs(arr);
    }

    private static void recycle(LocalCircuitBreaker local, int[] arr) {
        local.addEstimateBytesAndMaybeBreak(LocalBlockPool.usedBytesForInts(arr.length), "test");
        local.pool().recycleInts(arr);
    }

    private static void recycle(LocalCircuitBreaker local, double[] arr) {
        local.addEstimateBytesAndMaybeBreak(LocalBlockPool.usedBytesForDoubles(arr.length), "test");
        local.pool().recycleDoubles(arr);
    }

    public void testTrackMemoryUsage() {
        CircuitBreaker breaker = blockFactory().breaker();
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        long[] longs = new long[between(MIN_LONG_LENGTH, MAX_LONG_LENGTH)];
        int[] ints = new int[between(MIN_INT_LENGTH, MAX_INT_LENGTH)];
        double[] doubles = new double[between(MIN_LONG_LENGTH, MAX_LONG_LENGTH)];
        long bytes = (long) longs.length * Long.BYTES + (long) ints.length * Integer.BYTES + (long) doubles.length * Double.BYTES;
        recycle(local, longs);
        recycle(local, ints);
        recycle(local, doubles);
        assertThat(pool.idleBytes(), equalTo(bytes));
        assertThat(breaker.getUsed(), equalTo(bytes));

        assertThat(pool.pollLongs(between(MIN_LONG_LENGTH, longs.length)), sameInstance(longs));
        assertThat(pool.pollInts(between(MIN_INT_LENGTH, ints.length)), sameInstance(ints));
        assertThat(pool.pollDoubles(between(MIN_LONG_LENGTH, doubles.length)), sameInstance(doubles));
        assertThat(pool.pollLongs(MIN_LONG_LENGTH), nullValue());
        assertThat(pool.idleBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(bytes));

        local.addWithoutBreaking(-bytes);
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReuseDoesNotCallTheBreaker() {
        var breaker = new LocalCircuitBreakerTests.TrackingCircuitBreaker(blockFactory().breaker());
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        long[] longs = new long[MIN_LONG_LENGTH];
        int[] ints = new int[MIN_INT_LENGTH];
        double[] doubles = new double[MIN_LONG_LENGTH];
        recycle(local, longs);
        recycle(local, ints);
        recycle(local, doubles);
        int calls = breaker.callTimes();
        for (int i = 0; i < 10; i++) {
            assertThat(pool.pollLongs(longs.length), sameInstance(longs));
            assertThat(pool.pollInts(ints.length), sameInstance(ints));
            assertThat(pool.pollDoubles(doubles.length), sameInstance(doubles));
            pool.recycleLongs(longs);
            pool.recycleInts(ints);
            pool.recycleDoubles(doubles);
        }
        assertThat(breaker.callTimes(), equalTo(calls));
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReleasesArrayThatIsTooSmall() {
        CircuitBreaker breaker = blockFactory().breaker();
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        recycle(local, new long[2000]);
        recycle(local, new int[2000]);
        recycle(local, new double[2000]);
        assertThat(breaker.getUsed(), equalTo(pool.idleBytes()));
        assertThat(pool.pollLongs(2001), nullValue());
        assertThat(pool.pollInts(2001), nullValue());
        assertThat(pool.pollDoubles(2001), nullValue());
        assertThat(pool.idleBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(pool.pollLongs(2000), nullValue());
        assertThat(pool.pollInts(2000), nullValue());
        assertThat(pool.pollDoubles(2000), nullValue());
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReleasesArraysOutsideTheLimits() {
        CircuitBreaker breaker = blockFactory().breaker();
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        recycle(local, new long[MIN_LONG_LENGTH - 1]);
        recycle(local, new int[MIN_INT_LENGTH - 1]);
        recycle(local, new double[MIN_LONG_LENGTH - 1]);
        recycle(local, new long[MAX_LONG_LENGTH + 1]);
        recycle(local, new int[MAX_INT_LENGTH + 1]);
        recycle(local, new double[MAX_LONG_LENGTH + 1]);
        assertThat(pool.idleBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));

        recycle(local, new long[MAX_LONG_LENGTH]);
        recycle(local, new int[MAX_INT_LENGTH]);
        recycle(local, new double[MAX_LONG_LENGTH]);
        assertThat(pool.pollLongs(MIN_LONG_LENGTH - 1), nullValue());
        assertThat(pool.pollInts(MIN_INT_LENGTH - 1), nullValue());
        assertThat(pool.pollDoubles(MIN_LONG_LENGTH - 1), nullValue());
        assertThat(pool.pollLongs(MAX_LONG_LENGTH + 1), nullValue());
        assertThat(pool.pollInts(MAX_INT_LENGTH + 1), nullValue());
        assertThat(pool.pollDoubles(MAX_LONG_LENGTH + 1), nullValue());
        assertThat(pool.idleBytes(), equalTo(3 * LocalBlockPool.MAX_ARRAY_BYTES));
        assertThat(breaker.getUsed(), equalTo(pool.idleBytes()));
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testLimitsSlots() {
        CircuitBreaker breaker = blockFactory().breaker();
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        for (int i = 0; i < LocalBlockPool.SLOTS + 10; i++) {
            recycle(local, new long[MIN_LONG_LENGTH]);
            recycle(local, new int[MIN_INT_LENGTH]);
            recycle(local, new double[MIN_LONG_LENGTH]);
        }
        assertThat(pool.idleBytes(), equalTo(3 * LocalBlockPool.SLOTS * LocalBlockPool.MIN_ARRAY_BYTES));
        assertThat(breaker.getUsed(), equalTo(pool.idleBytes()));
        for (int i = 0; i < LocalBlockPool.SLOTS; i++) {
            assertThat(pool.pollLongs(MIN_LONG_LENGTH), not(nullValue()));
            assertThat(pool.pollInts(MIN_INT_LENGTH), not(nullValue()));
            assertThat(pool.pollDoubles(MIN_LONG_LENGTH), not(nullValue()));
        }
        assertThat(pool.pollLongs(MIN_LONG_LENGTH), nullValue());
        assertThat(pool.pollInts(MIN_INT_LENGTH), nullValue());
        assertThat(pool.pollDoubles(MIN_LONG_LENGTH), nullValue());
        assertThat(pool.idleBytes(), equalTo(0L));
        local.addWithoutBreaking(-3 * LocalBlockPool.SLOTS * LocalBlockPool.MIN_ARRAY_BYTES);
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testLimitsIdleBytes() {
        CircuitBreaker breaker = blockFactory().breaker();
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        LocalBlockPool pool = local.pool();
        for (int i = 0; i < 20; i++) {
            recycle(local, new long[MAX_LONG_LENGTH]);
        }
        assertThat(
            pool.idleBytes(),
            equalTo(LocalBlockPool.MAX_IDLE_BYTES / LocalBlockPool.MAX_ARRAY_BYTES * LocalBlockPool.MAX_ARRAY_BYTES)
        );
        assertThat(breaker.getUsed(), equalTo(pool.idleBytes()));
        local.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testPooledVectorReturnsItsArray() {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory factory = parent.newChildFactory(local);
        int size = between(MIN_LONG_LENGTH, MAX_LONG_LENGTH);
        long[] values = factory.pollLongs(size);
        long arrayBytes = LocalBlockPool.usedBytesForLongs(values.length);
        assertThat(parent.breaker().getUsed(), equalTo(arrayBytes));
        LongVector vector = factory.newLongArrayVector(values, size, arrayBytes);
        vector.pooled(true);
        assertThat(parent.breaker().getUsed(), equalTo(vector.ramBytesUsed()));
        vector.close();
        assertThat(local.pool().idleBytes(), equalTo(arrayBytes));
        assertThat(parent.breaker().getUsed(), equalTo(arrayBytes));

        long[] again = factory.pollLongs(size);
        assertThat(again, sameInstance(values));
        assertThat(parent.breaker().getUsed(), equalTo(arrayBytes));
        factory.recycleLongs(again);
        local.close();
        assertThat(parent.breaker().getUsed(), equalTo(0L));
    }

    public void testPooledVectorBlockReturnsItsArray() {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory factory = parent.newChildFactory(local);
        int size = between(MIN_LONG_LENGTH, MAX_LONG_LENGTH);
        double[] values = factory.pollDoubles(size);
        DoubleBlock block = factory.newDoubleArrayVector(values, size, LocalBlockPool.usedBytesForDoubles(values.length)).asBlock();
        block.pooled(true);
        block.close();
        assertThat(factory.pollDoubles(size), sameInstance(values));
        factory.recycleDoubles(values);
        local.close();
        assertThat(parent.breaker().getUsed(), equalTo(0L));
    }

    public void testPooledBlockWithNullsReturnsItsArray() {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory factory = parent.newChildFactory(local);
        int size = between(MIN_INT_LENGTH, MAX_INT_LENGTH);
        int[] values = factory.pollInts(size);
        long arrayBytes = LocalBlockPool.usedBytesForInts(values.length);
        BitSet nulls = new BitSet();
        nulls.set(between(0, size - 1));
        IntBlock block = factory.newIntArrayBlock(values, size, null, nulls, Block.MvOrdering.UNORDERED, arrayBytes);
        block.pooled(true);
        assertThat(parent.breaker().getUsed(), equalTo(block.ramBytesUsed()));
        block.close();
        assertThat(parent.breaker().getUsed(), equalTo(arrayBytes));

        assertThat(factory.pollInts(size), sameInstance(values));
        factory.recycleInts(values);
        local.close();
        assertThat(parent.breaker().getUsed(), equalTo(0L));
    }

    public void testPlainVectorKeepsItsArray() {
        BlockFactory parent = blockFactory();
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent.breaker(), 0, 0);
        BlockFactory factory = parent.newChildFactory(local);
        int size = between(MIN_LONG_LENGTH, MAX_LONG_LENGTH);
        factory.newLongArrayVector(new long[size], size).close();
        assertThat(local.pool().idleBytes(), equalTo(0L));
        assertThat(parent.breaker().getUsed(), equalTo(0L));
        local.close();
    }

    public void testPooledVectorPassedToDifferentDriverKeepsItsArray() {
        var breaker = new LocalCircuitBreakerTests.TrackingCircuitBreaker(blockFactory().breaker());
        BlockFactory parent = new BlockFactory(breaker, blockFactory().bigArrays());
        LocalCircuitBreaker local = new LocalCircuitBreaker(breaker, 0, 0);
        BlockFactory factory = parent.newChildFactory(local);
        int size = between(MIN_LONG_LENGTH, MAX_LONG_LENGTH);
        double[] values = factory.pollDoubles(size);
        DoubleVector vector = factory.newDoubleArrayVector(values, size, LocalBlockPool.usedBytesForDoubles(values.length));
        vector.pooled(true);
        vector.allowPassingToDifferentDriver();
        int calls = breaker.callTimes();
        vector.close();
        assertThat(breaker.callTimes(), equalTo(calls + 1));
        assertThat(local.pool().idleBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        local.close();
    }

    public void testPooledVectorWithoutLocalBreaker() {
        BlockFactory factory = blockFactory();
        int size = between(MIN_LONG_LENGTH, MAX_LONG_LENGTH);
        long[] values = factory.pollLongs(size);
        long arrayBytes = LocalBlockPool.usedBytesForLongs(values.length);
        assertThat(factory.breaker().getUsed(), equalTo(arrayBytes));
        LongVector vector = factory.newLongArrayVector(values, size, arrayBytes);
        vector.pooled(true);
        vector.close();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
        long[] again = factory.pollLongs(size);
        assertThat(again, not(sameInstance(values)));
        factory.recycleLongs(again);
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }
}
