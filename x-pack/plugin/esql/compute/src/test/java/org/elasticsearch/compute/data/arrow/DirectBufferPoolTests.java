/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class DirectBufferPoolTests extends ESTestCase {

    public void testBorrowReturnLifecycle() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            try {
                ArrowBuf first = pool.borrow(allocator, 64);
                long addr = first.memoryAddress();
                pool.returnBuf(first);
                ArrowBuf second = pool.borrow(allocator, 64);
                assertThat(second, sameInstance(first));
                assertEquals(addr, second.memoryAddress());
                assertEquals(first.capacity(), allocator.getAllocatedMemory());
                pool.returnBuf(second);
            } finally {
                pool.close();
            }
        }
    }

    public void testBorrowGrowsWhenTooSmall() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            try {
                ArrowBuf small = pool.borrow(allocator, 64);
                pool.returnBuf(small);
                ArrowBuf large = pool.borrow(allocator, 256);
                assertThat(large.capacity(), greaterThanOrEqualTo(256L));
                // Undersized idle buffer was closed; only the replacement is live.
                assertEquals(large.capacity(), allocator.getAllocatedMemory());
                pool.returnBuf(large);
            } finally {
                pool.close();
            }
        }
    }

    public void testMaxPooledEnforced() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            try {
                List<ArrowBuf> held = new ArrayList<>(DirectBufferPool.MAX_POOLED + 1);
                for (int i = 0; i < DirectBufferPool.MAX_POOLED + 1; i++) {
                    held.add(pool.borrow(allocator, 64));
                }
                long allLive = allocator.getAllocatedMemory();
                assertThat(allLive, greaterThan((long) DirectBufferPool.MAX_POOLED * 64));
                for (ArrowBuf buf : held) {
                    pool.returnBuf(buf);
                }
                assertEquals("excess return is freed, not parked", allLive - 64, allocator.getAllocatedMemory());
            } finally {
                pool.close();
            }
        }
    }

    public void testCloseFreesAll() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            int n = randomIntBetween(1, DirectBufferPool.MAX_POOLED);
            List<ArrowBuf> held = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                held.add(pool.borrow(allocator, 64));
            }
            for (ArrowBuf buf : held) {
                pool.returnBuf(buf);
            }
            assertThat(allocator.getAllocatedMemory(), greaterThan(0L));
            pool.close();
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    public void testReleaseIdleKeepsPoolOpen() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            ArrowBuf first = pool.borrow(allocator, 64);
            pool.returnBuf(first);
            pool.releaseIdle();
            assertEquals(0L, allocator.getAllocatedMemory());
            ArrowBuf second = pool.borrow(allocator, 64);
            assertThat(second.capacity(), greaterThanOrEqualTo(64L));
            pool.returnBuf(second);
            pool.close();
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    public void testReturnAfterCloseFrees() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            ArrowBuf buf = pool.borrow(allocator, 64);
            pool.close();
            pool.returnBuf(buf);
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    public void testConcurrentBorrowReturn() throws Exception {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            startInParallel(4, i -> {
                for (int n = 0; n < 50; n++) {
                    ArrowBuf buf = pool.borrow(allocator, 64 + (n % 3) * 64);
                    pool.returnBuf(buf);
                }
            });
            assertThat(allocator.getAllocatedMemory(), lessThanOrEqualTo((long) DirectBufferPool.MAX_POOLED * 192));
            pool.close();
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    public void testDirectBuffersDelegatesBorrowAndRelease() {
        DirectBufferPool pool = new DirectBufferPool();
        try (var allocator = DirectBufferAllocationManager.createRootAllocator(AllocationListener.NOOP, Long.MAX_VALUE)) {
            DirectBuffers buffers = new DirectBuffers(allocator, pool);
            ArrowBuf first = buffers.borrow(64);
            buffers.returnBuf(first);
            ArrowBuf second = buffers.borrow(64);
            assertThat(second, sameInstance(first));
            buffers.returnBuf(second);
            buffers.releaseIdle();
            assertEquals(0L, allocator.getAllocatedMemory());
            pool.close();
        }
    }
}
