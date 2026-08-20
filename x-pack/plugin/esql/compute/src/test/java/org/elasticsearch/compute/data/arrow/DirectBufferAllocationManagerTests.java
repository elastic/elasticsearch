/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DirectBufferAllocationManagerTests extends ESTestCase {

    private static final long FOUR_MB = 4L << 20;

    public void testAllocateAddressAndRoundTrip() {
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            try (ArrowBuf buf = allocator.buffer(64)) {
                assertThat(buf.memoryAddress(), greaterThan(0L));
                buf.setByte(0, (byte) 42);
                buf.setByte(63, (byte) 7);
                assertEquals(42, buf.getByte(0));
                assertEquals(7, buf.getByte(63));
                assertTrue(buf.nioBuffer().isDirect());
            }
        }
    }

    public void testReleaseDropsDirectBufferPoolUsage() {
        long before = directMemoryUsedBytes();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            ArrowBuf buf = allocator.buffer(FOUR_MB);
            long held = directMemoryUsedBytes();
            assertThat("allocateDirect must show up on the direct BufferPoolMXBean", held - before, greaterThanOrEqualTo(FOUR_MB));
            buf.close();
            long after = directMemoryUsedBytes();
            assertThat(
                "cleaner must return direct memory without waiting for GC; grew " + (after - before) + " after close",
                after - before,
                lessThanOrEqualTo(1L << 20)
            );
        }
    }

    public void testOverAllocatorMaxThrowsCircuitBreakingException() {
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(new CircuitBreakerAllocationListener(breaker), 1024)) {
            CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> allocator.buffer(2048));
            assertThat(e.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testFactoryOutOfMemoryErrorUndoesBreaker() {
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(64));
        AllocationManager.Factory oomFactory = new AllocationManager.Factory() {
            @Override
            public AllocationManager create(BufferAllocator accountingAllocator, long size) {
                throw DirectBufferAllocationManager.failedDirectAllocation(
                    accountingAllocator,
                    size,
                    new OutOfMemoryError("Direct buffer memory")
                );
            }

            @Override
            public ArrowBuf empty() {
                return DirectBufferAllocationManager.FACTORY.empty();
            }
        };
        try (
            RootAllocator allocator = DirectBufferAllocationManager.createRootAllocator(
                new CircuitBreakerAllocationListener(breaker),
                Long.MAX_VALUE,
                oomFactory
            )
        ) {
            CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> allocator.buffer(64));
            assertThat(e.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
            assertThat(e.getCause(), instanceOf(OutOfMemoryError.class));
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testSizeAboveIntegerMaxThrowsCircuitBreakingException() {
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            long tooBig = Integer.MAX_VALUE + 1L;
            CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> allocator.buffer(tooBig));
            assertThat(e.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
            assertThat(e.getBytesWanted(), equalTo(tooBig));
            assertThat(e.getByteLimit(), equalTo((long) Integer.MAX_VALUE));
        }
    }

    public void testArrowDirectMemoryLimitMatchesJvmInfo() {
        long maxDirect = JvmInfo.jvmInfo().getMem().getDirectMemoryMax().getBytes();
        long limit = DirectBufferAllocationManager.arrowDirectMemoryLimit();
        if (maxDirect <= 0L) {
            assertEquals(Long.MAX_VALUE, limit);
        } else {
            assertEquals(Math.max(0L, maxDirect - DirectBufferAllocationManager.NIO_RESERVE_BYTES), limit);
        }
    }

    public void testRequestBreakerTripsBeforeDirectCap() {
        var breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(1024));
        try (
            var allocator = DirectBufferAllocationManager.createRootAllocator(
                new CircuitBreakerAllocationListener(breaker),
                DirectBufferAllocationManager.arrowDirectMemoryLimit()
            )
        ) {
            expectThrows(CircuitBreakingException.class, () -> allocator.buffer(2048));
            assertEquals(0, breaker.getUsed());
        }
    }

    private static long directMemoryUsedBytes() {
        for (BufferPoolMXBean p : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(p.getName())) {
                return p.getMemoryUsed();
            }
        }
        fail("JVM has no 'direct' BufferPoolMXBean");
        return 0;
    }
}
