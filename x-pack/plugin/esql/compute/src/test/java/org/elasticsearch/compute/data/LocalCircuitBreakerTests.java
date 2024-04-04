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
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class LocalCircuitBreakerTests extends ESTestCase {

    static class TrackingCircuitBreaker implements CircuitBreaker {
        private final CircuitBreaker breaker;
        private final AtomicInteger called = new AtomicInteger();

        TrackingCircuitBreaker(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {

        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            called.incrementAndGet();
            breaker.addEstimateBytesAndMaybeBreak(bytes, label);
        }

        @Override
        public void checkRealMemoryUsage(String label) throws CircuitBreakingException {
            called.incrementAndGet();
            breaker.checkRealMemoryUsage(label);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            called.incrementAndGet();
            breaker.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return breaker.getUsed();
        }

        @Override
        public long getLimit() {
            return breaker.getLimit();
        }

        @Override
        public double getOverhead() {
            return breaker.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return breaker.getTrippedCount();
        }

        @Override
        public String getName() {
            return breaker.getName();
        }

        @Override
        public Durability getDurability() {
            return breaker.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            breaker.setLimitAndOverhead(limit, overhead);
        }

        int callTimes() {
            return called.get();
        }
    }

    private TrackingCircuitBreaker newTestBreaker(long limit) {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(limit)).withCircuitBreaking();
        return new TrackingCircuitBreaker(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
    }

    public void testBasic() {
        TrackingCircuitBreaker breaker = newTestBreaker(120);
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(breaker, 30, 50);
        localBreaker.addEstimateBytesAndMaybeBreak(20, "test");
        assertThat(localBreaker.getReservedBytes(), equalTo(30L));
        assertThat(breaker.callTimes(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(50L));
        localBreaker.addWithoutBreaking(-5);
        assertThat(breaker.getUsed(), equalTo(50L));
        assertThat(localBreaker.getReservedBytes(), equalTo(35L));
        localBreaker.addEstimateBytesAndMaybeBreak(25, "test");
        assertThat(breaker.getUsed(), equalTo(50L));
        assertThat(breaker.callTimes(), equalTo(1));
        assertThat(localBreaker.getReservedBytes(), equalTo(10L));
        var error = expectThrows(CircuitBreakingException.class, () -> localBreaker.addEstimateBytesAndMaybeBreak(60, "test"));
        assertThat(error.getBytesWanted(), equalTo(80L));
        assertThat(breaker.callTimes(), equalTo(2));
        localBreaker.addEstimateBytesAndMaybeBreak(30, "test");
        assertThat(breaker.getUsed(), equalTo(100L));
        assertThat(localBreaker.getReservedBytes(), equalTo(30L));
        assertThat(breaker.callTimes(), equalTo(3));
        localBreaker.addWithoutBreaking(-40L);
        assertThat(breaker.getUsed(), equalTo(80L));
        assertThat(localBreaker.getReservedBytes(), equalTo(50L));
        assertThat(breaker.callTimes(), equalTo(4));
        localBreaker.close();
        assertThat(breaker.getUsed(), equalTo(30L));
    }
}
