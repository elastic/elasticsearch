/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;

public class CircuitBreakerAllocationListenerTests extends ESTestCase {

    private CircuitBreaker breaker(long limit) {
        return new TestBreaker(limit);
    }

    private BufferAllocator allocator(CircuitBreaker breaker) {
        return new RootAllocator(new CircuitBreakerAllocationListener(breaker), Long.MAX_VALUE);
    }

    public void testAllocationWithinLimitSucceeds() {
        var breaker = breaker(1024);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf = allocator.buffer(512);
            assertEquals(512, breaker.getUsed());
            buf.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testLargeAllocationTripsBreaker() {
        var breaker = breaker(1024);
        try (var allocator = allocator(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> allocator.buffer(2048));
        }
    }

    public void testMultipleAllocationsExceedingLimitTripsBreaker() {
        var breaker = breaker(2048);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(1024);
            assertEquals(1024, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(512);
            assertEquals(1536, breaker.getUsed());

            // This allocation should push us over the limit
            expectThrows(CircuitBreakingException.class, () -> allocator.buffer(1024));

            buf2.close();
            buf1.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testReleaseDecrementsBreaker() {
        var breaker = breaker(4096);
        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(1024);
            assertEquals(1024, breaker.getUsed());

            // Release and re-allocate — breaker should track correctly
            buf1.close();
            assertEquals(0, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(2048);
            assertEquals(2048, breaker.getUsed());
            buf2.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testAllocationAfterReleaseSucceeds() {
        var breaker = breaker(1024);

        try (var allocator = allocator(breaker)) {
            ArrowBuf buf1 = allocator.buffer(800);
            // Allocation can be larger than the requested size.
            // (the real value is 1024, i.e. a power of 2)
            assertTrue(breaker.getUsed() >= 800);

            // Would exceed limit if buf1 is still held
            expectThrows(Exception.class, () -> allocator.buffer(800));

            // Free buf1 and try again — should succeed now
            buf1.close();
            assertEquals(0, breaker.getUsed());

            ArrowBuf buf2 = allocator.buffer(800);
            assertTrue(breaker.getUsed() >= 800);
            buf2.close();
            assertEquals(0, breaker.getUsed());
        }
    }

    static class TestBreaker implements CircuitBreaker {
        private long limit;
        private long used = 0;

        TestBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException(fieldName, bytesNeeded, limit, Durability.TRANSIENT);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (used + bytes > limit) {
                throw new CircuitBreakingException(label, bytes, limit, Durability.TRANSIENT);
            }
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public double getOverhead() {
            return 0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return "test breaker";
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            this.limit = limit;
        }
    }
}
