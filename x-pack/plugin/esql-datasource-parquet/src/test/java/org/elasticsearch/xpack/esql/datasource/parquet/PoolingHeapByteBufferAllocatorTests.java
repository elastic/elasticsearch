/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
        long seed = randomLong();

        startInParallel(threads, t -> {
            Random rng = new Random(seed + t);
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

    /**
     * A checkout that parquet-mr never releases must not stay pinned by the node-wide pool: once
     * the buffer is unreachable, its tracking entry is purged, the leak is counted, and the backing
     * array becomes plain garbage — it must never re-enter the free list.
     */
    public void testMissedReleaseIsCollectedNotPinned() throws Exception {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        WeakReference<byte[]> leakedArray = allocateAndLeak(pool);
        assertEquals(1, pool.checkedOutCount());
        assertBusy(() -> {
            System.gc();
            assertEquals("an unreleased checkout must be purged once collected", 0, pool.checkedOutCount());
            assertEquals(1, pool.leakedCheckouts());
            assertNull("the leaked backing array must be GC-reclaimable, not pinned", leakedArray.get());
        });
        assertEquals("a leaked array must not re-enter the pool", 0, pool.pooledCount());
        ByteBuffer next = pool.allocate(64);
        pool.release(next);
        assertEquals("the pool must keep working after a purged leak", 1, pool.pooledCount());
    }

    /**
     * Extracted so the checkout goes unreachable when this frame returns; keeping the allocation in
     * the test body would leave a stack reference that defeats the GC assertion.
     */
    private static WeakReference<byte[]> allocateAndLeak(PoolingHeapByteBufferAllocator pool) {
        ByteBuffer leaked = pool.allocate(64);
        return new WeakReference<>(leaked.array());
    }

    /**
     * Each size class may keep at most a quarter of the cap idle, so a burst of footer-sized
     * arrays cannot evict every smaller class from the pool.
     */
    public void testPerClassLimitPreventsLargeClassMonopoly() {
        long cap = 4L * PoolingHeapByteBufferAllocator.MAX_POOLED;
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(cap);
        assertEquals(1, pool.classEntryLimit(PoolingHeapByteBufferAllocator.MAX_POOLED));

        ByteBuffer first = pool.allocate(PoolingHeapByteBufferAllocator.MAX_POOLED);
        ByteBuffer second = pool.allocate(PoolingHeapByteBufferAllocator.MAX_POOLED);
        pool.release(first);
        pool.release(second);
        assertEquals("the class limit must drop the second large array despite global cap headroom", 1, pool.pooledCount());
        assertEquals(PoolingHeapByteBufferAllocator.MAX_POOLED, pool.pooledBytes());
        assertEquals(1, pool.droppedReleases());

        ByteBuffer small = pool.allocate(64);
        pool.release(small);
        assertEquals("smaller classes must still pool", 2, pool.pooledCount());
    }

    public void testStatsTrackHitsMissesAndBypass() {
        PoolingHeapByteBufferAllocator pool = new PoolingHeapByteBufferAllocator(1 << 20);
        ByteBuffer first = pool.allocate(300);
        pool.release(first);
        ByteBuffer second = pool.allocate(400); // same 512-byte class: must reuse
        pool.release(second);
        assertEquals(1, pool.poolHits());
        assertEquals(1, pool.poolMisses());
        assertEquals(0, pool.bypassedAllocations());

        int tooBig = PoolingHeapByteBufferAllocator.MAX_POOLED + 1;
        ByteBuffer big = pool.allocate(tooBig);
        pool.release(big);
        assertEquals(1, pool.bypassedAllocations());
        assertEquals(tooBig, pool.bypassedBytes());
        assertEquals(tooBig, pool.maxRequested());
        assertEquals(0, pool.leakedCheckouts());
    }

    /**
     * The two large allocation classes parquet-mr routes through the read-options allocator — the
     * 8 MiB {@code maxAllocationSize} chunk slabs of {@code PlainParquetReadOptions} and footer
     * buffers of up to {@link ParquetFormatReader#MAX_FOOTER_READ_BYTES} — are exactly the
     * allocations that scale with file count; they must be poolable or the pool misses its point.
     */
    public void testPoolCoversParquetLargeAllocationClasses() {
        assertTrue(PoolingHeapByteBufferAllocator.targetShift(8 * 1024 * 1024) >= 0);
        assertTrue(PoolingHeapByteBufferAllocator.targetShift(ParquetFormatReader.MAX_FOOTER_READ_BYTES) >= 0);
    }

    public void testForHeapCapIsHeapOverDivisor() {
        PoolingHeapByteBufferAllocator pool = PoolingHeapByteBufferAllocator.forHeap();
        long heapBytes = org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertEquals(Math.max(1L, heapBytes / PoolingHeapByteBufferAllocator.HEAP_DIVISOR), pool.cap());
    }
}
