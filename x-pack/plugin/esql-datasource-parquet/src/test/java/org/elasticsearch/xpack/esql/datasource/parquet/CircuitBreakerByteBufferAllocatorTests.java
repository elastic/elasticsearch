/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

public class CircuitBreakerByteBufferAllocatorTests extends ESTestCase {

    private CircuitBreaker breaker(long limit) {
        return new LimitedBreaker("test", ByteSizeValue.ofBytes(limit));
    }

    private ByteBufferAllocator allocator(CircuitBreaker breaker) {
        return new CircuitBreakerByteBufferAllocator(new TestAllocator(0, Integer.MAX_VALUE), breaker);
    }

    private ByteBufferAllocator allocator(CircuitBreaker breaker, int minAllocationSize, int maxAllocationSize) {
        return new CircuitBreakerByteBufferAllocator(new TestAllocator(minAllocationSize, maxAllocationSize), breaker);
    }

    public void testAllocationWithinLimitSucceeds() {
        var breaker = breaker(1024);
        var allocator = allocator(breaker);
        ByteBuffer buf = allocator.allocate(512);
        assertEquals(512, breaker.getUsed());
        allocator.release(buf);
        assertEquals(0, breaker.getUsed());
    }

    public void testLargeAllocationTripsBreaker() {
        var breaker = breaker(1024);
        var allocator = allocator(breaker);
        expectThrows(CircuitBreakingException.class, () -> allocator.allocate(2048));
    }

    public void testMultipleAllocationsExceedingLimitTripsBreaker() {
        var breaker = breaker(2048);
        var allocator = allocator(breaker);
        ByteBuffer buf1 = allocator.allocate(1024);
        assertEquals(1024, breaker.getUsed());

        ByteBuffer buf2 = allocator.allocate(512);
        assertEquals(1536, breaker.getUsed());

        // This allocation should push us over the limit
        expectThrows(CircuitBreakingException.class, () -> allocator.allocate(1024));

        allocator.release(buf2);
        allocator.release(buf1);
        assertEquals(0, breaker.getUsed());
    }

    public void testReleaseDecrementsBreaker() {
        var breaker = breaker(4096);
        var allocator = allocator(breaker);
        ByteBuffer buf1 = allocator.allocate(1024);
        assertEquals(1024, breaker.getUsed());

        // Release and re-allocate — breaker should track correctly
        allocator.release(buf1);
        assertEquals(0, breaker.getUsed());

        ByteBuffer buf2 = allocator.allocate(2048);
        assertEquals(2048, breaker.getUsed());
        allocator.release(buf2);
        assertEquals(0, breaker.getUsed());
    }

    public void testAllocationAfterReleaseSucceeds() {
        var breaker = breaker(1024);

        var allocator = allocator(breaker);
        ByteBuffer buf1 = allocator.allocate(800);
        // Allocation can be larger than the requested size.
        // (the real value is 1024, i.e. a power of 2)
        assertTrue(breaker.getUsed() >= 800);

        // Would exceed limit if buf1 is still held
        expectThrows(Exception.class, () -> allocator.allocate(800));

        // Free buf1 and try again — should succeed now
        allocator.release(buf1);
        assertEquals(0, breaker.getUsed());

        ByteBuffer buf2 = allocator.allocate(800);
        assertTrue(breaker.getUsed() >= 800);
        allocator.release(buf2);
        assertEquals(0, breaker.getUsed());
    }

    public void testBelowMinAllocation() {
        var breaker = breaker(200);
        var allocator = allocator(breaker, 100, 300);
        var buffer = allocator.allocate(50);
        // Check that over-allocation happened
        assertEquals(100, buffer.capacity());
        allocator.release(buffer);
        assertEquals(0, breaker.getUsed());
    }

    public void testAboveMaxAllocation() {
        var breaker = breaker(100);
        var allocator = allocator(breaker, 200, 300);
        // First reserves 50 bytes, then extra rounded capacity (+150 bytes) trips the breaker
        expectThrows(CircuitBreakingException.class, () -> allocator.allocate(50));
        assertEquals(0, breaker.getUsed());
    }

    public void testAllocationFails() {
        var breaker = breaker(300);
        var allocator = allocator(breaker, 0, 100);
        // Breaker accepts reservation, allocation fails
        expectThrows(IllegalArgumentException.class, () -> allocator.allocate(200));
        assertEquals(0, breaker.getUsed());
    }

    static class TestAllocator implements ByteBufferAllocator {
        private final int minAllocationSize;
        private final int maxAllocationSize;
        private final ByteBufferAllocator delegate = new HeapByteBufferAllocator();

        TestAllocator(int minAllocationSize, int maxAllocationSize) {
            this.minAllocationSize = minAllocationSize;
            this.maxAllocationSize = maxAllocationSize;
        }

        @Override
        public ByteBuffer allocate(int size) {
            if (size > maxAllocationSize) {
                throw new IllegalArgumentException("Exceeded capacity");
            }
            return delegate.allocate(Math.max(size, minAllocationSize));
        }

        @Override
        public void release(ByteBuffer b) {}

        @Override
        public boolean isDirect() {
            return delegate.isDirect();
        }
    }
}
