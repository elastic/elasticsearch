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
        try (LongSingleArrayPool pool = new LongSingleArrayPool(breaker)) {
            long[] first = pool.getOrAllocate(10);
            assertThat(first.length, equalTo(10));
            pool.put(first);
            assertThat(pool.getOrAllocate(10), sameInstance(first));
            pool.put(first);
            assertThat(pool.getOrAllocate(3), sameInstance(first));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testAllocatesWhileTheArrayIsInUse() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (IntSingleArrayPool pool = new IntSingleArrayPool(breaker)) {
            int[] first = pool.getOrAllocate(10);
            long used = breaker.getUsed();
            assertThat(used, greaterThan(0L));
            int[] second = pool.getOrAllocate(10);
            assertThat(second, not(sameInstance(first)));
            assertThat(breaker.getUsed(), equalTo(used));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testGrows() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (DoubleSingleArrayPool pool = new DoubleSingleArrayPool(breaker)) {
            double[] small = pool.getOrAllocate(10);
            long used = breaker.getUsed();
            pool.put(small);
            double[] large = pool.getOrAllocate(1000);
            assertThat(large.length, greaterThanOrEqualTo(1000));
            assertThat(breaker.getUsed(), greaterThan(used));
            long usedByLarge = breaker.getUsed();
            pool.put(large);
            assertThat(pool.getOrAllocate(10), sameInstance(large));
            assertThat(breaker.getUsed(), equalTo(usedByLarge));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreaks() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        try (LongSingleArrayPool pool = new LongSingleArrayPool(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> pool.getOrAllocate(1000));
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(pool.getOrAllocate(2).length, equalTo(2));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testVectorPutsTheArrayBack() {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofMb(1));
        try (LongSingleArrayPool pool = new LongSingleArrayPool(blockFactory.breaker())) {
            long[] values = pool.getOrAllocate(10);
            LongVector vector = blockFactory.newLongArrayVector(values, 10);
            vector.attachReleasable(() -> pool.put(values));
            LongBlock block = vector.asBlock();
            block.incRef();
            block.close();
            assertThat(pool.getOrAllocate(10), not(sameInstance(values)));
            block.close();
            assertThat(pool.getOrAllocate(10), sameInstance(values));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
