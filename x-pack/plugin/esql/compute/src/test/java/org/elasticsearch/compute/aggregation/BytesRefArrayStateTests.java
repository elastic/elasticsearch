/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies {@link BytesRefArrayState#close()} releases exactly the memory it reserved, and that it
 * does so via a single batched breaker call regardless of how many groups were populated -- not one
 * call per group.
 */
public class BytesRefArrayStateTests extends ESTestCase {

    public void testCloseReleasesAllReservedBytesInOneCall() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200)).withCircuitBreaking();
        var breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var countingBreaker = new ReleaseCountingCircuitBreaker(breaker);

        int numGroups = between(1, 50_000);
        boolean withNulls = randomBoolean();

        var state = new BytesRefArrayState(bigArrays, countingBreaker, "test");
        if (withNulls) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
        }
        // Group 0 is always set, guaranteeing the breaker holds >0 bytes before close() regardless
        // of how the random null-skipping below plays out.
        state.set(0, new BytesRef(randomByteArrayOfLength(randomIntBetween(0, 64))));
        for (int g = 1; g < numGroups; g++) {
            if (withNulls == false || randomBoolean()) {
                state.set(g, new BytesRef(randomByteArrayOfLength(randomIntBetween(0, 64))));
                if (randomBoolean()) {
                    // Re-set some groups too, exercising the grow/shrink breaker paths before close().
                    state.set(g, new BytesRef(randomByteArrayOfLength(randomIntBetween(0, 64))));
                }
            }
        }

        assertThat("breaker should be holding memory before close", breaker.getUsed(), greaterThan(0L));

        long releaseCallsBeforeClose = countingBreaker.releaseCalls.get();
        state.close();
        long releaseCallsDuringClose = countingBreaker.releaseCalls.get() - releaseCallsBeforeClose;

        assertThat("breaker must be fully released after close, regardless of numGroups=" + numGroups, breaker.getUsed(), equalTo(0L));
        assertThat(
            "close() should batch every group's release into a single breaker call, not one per group",
            releaseCallsDuringClose,
            equalTo(1L)
        );
    }

    public void testCloseWithNoValuesTouchesBreakerZeroTimes() {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200)).withCircuitBreaking();
        var breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var countingBreaker = new ReleaseCountingCircuitBreaker(breaker);

        var state = new BytesRefArrayState(bigArrays, countingBreaker, "test");
        state.close();

        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(countingBreaker.releaseCalls.get(), equalTo(0L));
    }

    /** Delegates to a real breaker, counting only release (negative-delta) calls to {@link #addWithoutBreaking(long)}. */
    private static class ReleaseCountingCircuitBreaker implements CircuitBreaker {
        private final CircuitBreaker delegate;
        final AtomicLong releaseCalls = new AtomicLong();

        ReleaseCountingCircuitBreaker(CircuitBreaker delegate) {
            this.delegate = delegate;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            if (bytes < 0) {
                releaseCalls.incrementAndGet();
            }
            delegate.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }
}
