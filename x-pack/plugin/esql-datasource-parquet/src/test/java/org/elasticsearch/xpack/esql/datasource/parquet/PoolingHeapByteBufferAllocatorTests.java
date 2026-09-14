/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PoolingHeapByteBufferAllocatorTests extends ESTestCase {

    public void testReusesBackingArrayNotBufferIdentity() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer first = pool.allocate(100);
        byte[] backing = first.array();
        int capacity = first.capacity();
        pool.release(first);
        ByteBuffer second = pool.allocate(100);
        try {
            assertNotSame(first, second);
            assertSame(backing, second.array());
            assertEquals(capacity, second.capacity());
            assertEquals(0, second.position());
            assertEquals(100, second.limit());
            assertEquals(ByteOrder.BIG_ENDIAN, second.order());
        } finally {
            pool.release(second);
        }
    }

    public void testResetsByteOrderOnCheckout() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer first = pool.allocate(64);
        first.order(ByteOrder.LITTLE_ENDIAN);
        byte[] backing = first.array();
        pool.release(first);
        ByteBuffer second = pool.allocate(64);
        try {
            assertSame(backing, second.array());
            assertEquals(ByteOrder.BIG_ENDIAN, second.order());
        } finally {
            pool.release(second);
        }
    }

    public void testCapDropsExcess() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(PoolingHeapByteBufferAllocator.MIN_POOLED);
        ByteBuffer a = pool.allocate(PoolingHeapByteBufferAllocator.MIN_POOLED);
        ByteBuffer b = pool.allocate(PoolingHeapByteBufferAllocator.MIN_POOLED);
        assertNotSame(a, b);
        pool.release(a);
        pool.release(b);
        assertTrue("idle pooled bytes must not exceed the cap", pool.pooledBytes() <= pool.cap());
        assertEquals(pool.cap(), pool.pooledBytes());
        assertEquals(1, pool.pooledCount());
    }

    public void testSliceReleaseDoesNotEnterPool() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer allocated = pool.allocate(64);
        ByteBuffer slice = allocated.slice();
        pool.release(slice);
        assertEquals(0, pool.pooledCount());
        pool.release(allocated);
        assertEquals(1, pool.pooledCount());
    }

    public void testDoubleReleaseDoesNotDuplicateIdentity() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer allocated = pool.allocate(64);
        byte[] backing = allocated.array();
        pool.release(allocated);
        pool.release(allocated);
        assertEquals(1, pool.pooledCount());
        ByteBuffer first = pool.allocate(64);
        ByteBuffer second = pool.allocate(64);
        try {
            assertSame(backing, first.array());
            assertNotSame(first.array(), second.array());
        } finally {
            pool.release(first);
            pool.release(second);
        }
    }

    public void testStaleReleaseAfterReuseDoesNotRePoolLiveArray() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer first = pool.allocate(64);
        byte[] backing = first.array();
        pool.release(first);
        ByteBuffer live = pool.allocate(64);
        assertSame(backing, live.array());
        pool.release(first);
        assertEquals("stale release must not enqueue a live checkout", 0, pool.pooledCount());
        assertEquals(1, pool.checkedOutCount());
        ByteBuffer other = pool.allocate(64);
        try {
            assertNotSame(live.array(), other.array());
        } finally {
            pool.release(live);
            pool.release(other);
        }
    }

    public void testSliceOnlyReleaseLeavesOriginalCheckedOut() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer allocated = pool.allocate(64);
        pool.release(allocated.slice());
        assertEquals(0, pool.pooledCount());
        assertEquals(1, pool.checkedOutCount());
        pool.release(allocated);
        assertEquals(1, pool.pooledCount());
        assertEquals(0, pool.checkedOutCount());
    }

    public void testNegativeSizeRejected() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1024);
        expectThrows(IllegalArgumentException.class, () -> pool.allocate(-1));
        expectThrows(IllegalArgumentException.class, () -> new PoolingHeapByteBufferAllocator(0));
    }

    public void testZeroSizeIsNotPooled() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer empty = pool.allocate(0);
        assertEquals(0, empty.capacity());
        pool.release(empty);
        assertEquals(0, pool.pooledCount());
    }

    public void testOversizedAllocationIsNotPooled() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1L << 30);
        int tooBig = PoolingHeapByteBufferAllocator.MAX_POOLED + 1;
        ByteBuffer large = pool.allocate(tooBig);
        assertEquals(tooBig, large.capacity());
        pool.release(large);
        assertEquals(0, pool.pooledCount());
        assertEquals(0, pool.pooledBytes());
    }

    public void testIsHeapBacked() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1024);
        assertFalse(pool.isDirect());
        ByteBuffer buffer = pool.allocate(8);
        try {
            assertTrue(buffer.hasArray());
            assertFalse(buffer.isDirect());
            assertEquals(0, buffer.arrayOffset());
        } finally {
            pool.release(buffer);
        }
    }

    public void testTargetShiftRoundsToPowerOfTwo() {
        assertEquals(-1, PoolingHeapByteBufferAllocator.targetShift(0));
        assertEquals(
            Integer.numberOfTrailingZeros(PoolingHeapByteBufferAllocator.MIN_POOLED),
            PoolingHeapByteBufferAllocator.targetShift(1)
        );
        assertEquals(8, PoolingHeapByteBufferAllocator.targetShift(256));
        assertEquals(9, PoolingHeapByteBufferAllocator.targetShift(257));
        assertEquals(-1, PoolingHeapByteBufferAllocator.targetShift(PoolingHeapByteBufferAllocator.MAX_POOLED + 1));
    }

    public void testConcurrentAllocateReleaseNeverExceedsCapOrHandsOutSameInstance() {
        int threads = 8;
        int ops = 200;
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(PoolingHeapByteBufferAllocator.MIN_POOLED * 4L);
        AtomicLong capViolations = new AtomicLong();
        AtomicInteger identityCollisions = new AtomicInteger();
        Set<ByteBuffer> live = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        startInParallel(threads, t -> {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            List<ByteBuffer> held = new ArrayList<>();
            for (int i = 0; i < ops; i++) {
                if (pool.pooledBytes() > pool.cap()) {
                    capViolations.incrementAndGet();
                }
                if (rng.nextBoolean() && held.isEmpty() == false) {
                    ByteBuffer buf = held.remove(held.size() - 1);
                    live.remove(buf);
                    pool.release(buf);
                } else {
                    ByteBuffer buf = pool.allocate(rng.nextInt(1, PoolingHeapByteBufferAllocator.MIN_POOLED + 1));
                    if (live.add(buf) == false) {
                        identityCollisions.incrementAndGet();
                    }
                    held.add(buf);
                }
            }
            for (ByteBuffer buf : held) {
                live.remove(buf);
                pool.release(buf);
            }
        });
        assertEquals(0, capViolations.get());
        assertEquals(0, identityCollisions.get());
        assertTrue(pool.pooledBytes() <= pool.cap());
    }

    public void testForHeapCapIsHeapOverDivisor() {
        PoolingHeapByteBufferAllocator pool = PoolingHeapByteBufferAllocator.forHeap();
        long heapBytes = org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertEquals(Math.max(1L, heapBytes / PoolingHeapByteBufferAllocator.HEAP_DIVISOR), pool.cap());
    }
}
