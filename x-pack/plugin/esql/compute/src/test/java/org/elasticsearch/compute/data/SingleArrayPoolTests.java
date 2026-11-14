/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SingleArrayPoolTests extends ESTestCase {
    public void testReusesTheArrayPutBack() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (SingleArrayPool pool = new SingleArrayPool(breaker, () -> {})) {
            long[] first = pool.getOrAllocateLongs(10);
            assertThat(first.length, equalTo(10));
            pool.putLongs(first);
            assertThat(pool.getOrAllocateLongs(10), sameInstance(first));
            pool.putLongs(first);
            assertThat(pool.getOrAllocateLongs(3), sameInstance(first));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testAllocatesWhileTheArrayIsInUse() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (SingleArrayPool pool = new SingleArrayPool(breaker, () -> {})) {
            int[] first = pool.getOrAllocateInts(10);
            long used = breaker.getUsed();
            assertThat(used, greaterThan(0L));
            int[] second = pool.getOrAllocateInts(10);
            assertThat(second, not(sameInstance(first)));
            assertThat(breaker.getUsed(), equalTo(used));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testGrows() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (SingleArrayPool pool = new SingleArrayPool(breaker, () -> {})) {
            double[] small = pool.getOrAllocateDoubles(10);
            long used = breaker.getUsed();
            pool.putDoubles(small);
            double[] large = pool.getOrAllocateDoubles(1000);
            assertThat(large.length, greaterThanOrEqualTo(1000));
            assertThat(breaker.getUsed(), greaterThan(used));
            long usedByLarge = breaker.getUsed();
            pool.putDoubles(large);
            assertThat(pool.getOrAllocateDoubles(10), sameInstance(large));
            assertThat(breaker.getUsed(), equalTo(usedByLarge));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreaks() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        try (SingleArrayPool pool = new SingleArrayPool(breaker, () -> {})) {
            expectThrows(CircuitBreakingException.class, () -> pool.getOrAllocateLongs(1000));
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(pool.getOrAllocateLongs(2).length, equalTo(2));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testVectorPutsTheArrayBack() {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofMb(1));
        try (SingleArrayPool pool = new SingleArrayPool(blockFactory.breaker(), () -> {})) {
            long[] values = pool.getOrAllocateLongs(10);
            LongVector vector = blockFactory.newLongArrayVector(values, 10);
            vector.attachReleasable(() -> pool.putLongs(values));
            LongBlock block = vector.asBlock();
            block.incRef();
            block.close();
            assertThat(pool.getOrAllocateLongs(10), not(sameInstance(values)));
            block.close();
            assertThat(pool.getOrAllocateLongs(10), sameInstance(values));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testEachTypeHasItsOwnArray() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        boolean[] closed = new boolean[1];
        try (SingleArrayPool pool = new SingleArrayPool(breaker, () -> closed[0] = true)) {
            assertThat(breaker.getUsed(), equalTo(0L));
            long[] longs = pool.getOrAllocateLongs(10);
            long usedByLongs = breaker.getUsed();
            int[] ints = pool.getOrAllocateInts(10);
            double[] doubles = pool.getOrAllocateDoubles(10);
            assertThat(breaker.getUsed(), greaterThan(usedByLongs));
            pool.putLongs(longs);
            pool.putInts(ints);
            pool.putDoubles(doubles);
            assertThat(pool.getOrAllocateLongs(10), sameInstance(longs));
            assertThat(pool.getOrAllocateInts(10), sameInstance(ints));
            assertThat(pool.getOrAllocateDoubles(10), sameInstance(doubles));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
        assertTrue(closed[0]);
    }
}
