/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Heap {@link ByteBufferAllocator} that reuses released backing arrays up to a heap-relative cap.
 * Shared by every derived {@link ParquetFormatReader} on a node, the same way as
 * {@link ParquetIoWatermark}, so sequential file opens reuse arrays instead of leaving a
 * file-count-scaled trail of dead {@code byte[]} for the parent breaker to trip on.
 *
 * <p>Idle pooled bytes are not charged to any query breaker; {@link #HEAP_DIVISOR} keeps that
 * unaccounted retained heap small relative to a default-profile parent limit (on a 4 GB heap,
 * {@code heap / 32} is 128 MB). In-use buffers are charged by
 * {@link CircuitBreakerByteBufferAllocator} wrapping this allocator.
 *
 * <p>Arrays are pooled in power-of-two size classes up to {@link #MAX_POOLED}, which is sized to
 * cover everything parquet-mr routes through the read-options allocator: chunk reads come in
 * {@code maxAllocationSize} slabs (8 MiB, see {@code PlainParquetReadOptions}) and footer parses
 * allocate the exact serialized footer length, which the footer fetch window bounds at
 * {@code ParquetFormatReader#MAX_FOOTER_READ_BYTES} (10 MiB). Each size class additionally keeps
 * at most a quarter of the cap idle, so a burst of footer-sized arrays cannot evict every smaller
 * class.
 *
 * <p>Each {@link #allocate} wraps a fresh {@link ByteBuffer} around a pooled (or new) {@code byte[]}.
 * Identities are not recycled, so a stale {@link #release} of a previous checkout cannot re-pool an
 * array another caller still holds. A slice or foreign buffer is ignored; parquet-mr must
 * {@link #release} the instance {@link #allocate} returned. Checkouts are tracked through
 * {@link WeakReference}s: a checkout that is never released becomes ordinary garbage once
 * unreachable — its array is dropped, never re-pooled (unreachability is not a release signal we
 * trust with possibly-aliased arrays) — and is counted in {@link #leakedCheckouts()} instead of
 * pinning heap for the life of this node-wide pool.
 */
final class PoolingHeapByteBufferAllocator implements ByteBufferAllocator {

    /**
     * Idle pooled bytes cap as a fraction of max heap. Smaller than {@link ParquetIoWatermark}'s
     * {@code heap / 8} because those I/O bytes are request-breaker charged and this idle set is not.
     * {@code / 32} on a default ~4 GB heap is 128 MB — enough to reuse typical footer/chunk-slab
     * arrays across hundreds of files, small enough that it cannot recreate the ~3.8 GB parent trip.
     */
    static final int HEAP_DIVISOR = 32;

    /** Smallest power-of-two size class. Below this, allocate this size and still pool. */
    static final int MIN_POOLED = 1 << 8;

    /**
     * Largest array that may enter the free list. Sized above the two large allocation classes
     * parquet-mr sends through the read-options allocator — 8 MiB {@code maxAllocationSize} chunk
     * slabs and footer buffers of up to the 10 MiB fetch window — so the very allocations that
     * scale with file count do not bypass the pool. Anything larger (rare) is exact-sized and
     * dropped on release.
     */
    static final int MAX_POOLED = 1 << 24;

    private static final int MIN_SHIFT = Integer.numberOfTrailingZeros(MIN_POOLED);
    private static final int MAX_SHIFT = Integer.numberOfTrailingZeros(MAX_POOLED);
    private static final int SIZE_CLASSES = MAX_SHIFT - MIN_SHIFT + 1;

    private final long cap;
    private final AtomicLong pooledBytes = new AtomicLong();
    private final List<ConcurrentLinkedQueue<byte[]>> free;
    /** Idle entries per size class, bounded by {@link #classEntryLimit}. */
    private final AtomicIntegerArray freeCounts = new AtomicIntegerArray(SIZE_CLASSES);
    /** Max idle entries per size class: a quarter of the cap, but always at least one array. */
    private final int[] classEntryLimit = new int[SIZE_CLASSES];

    /**
     * Identity of every {@link ByteBuffer} currently checked out, mapped to its backing array.
     * Keys are weak identity references (see {@link CheckoutRef}): while a checkout is reachable
     * only an exact-instance {@link #release} can re-pool its array; once it becomes unreachable
     * without a release, the entry is purged by {@link #expungeStaleCheckouts()} and the array is
     * left to GC.
     */
    private final ConcurrentHashMap<CheckoutRef, byte[]> inUse = new ConcurrentHashMap<>();
    private final ReferenceQueue<ByteBuffer> staleCheckouts = new ReferenceQueue<>();

    // Stats, primarily for tests: how the pool behaves against real parquet-mr traffic.
    private final AtomicLongArray allocationsByClass = new AtomicLongArray(SIZE_CLASSES);
    private final AtomicLong poolHits = new AtomicLong();
    private final AtomicLong poolMisses = new AtomicLong();
    private final AtomicLong bypassedAllocations = new AtomicLong();
    private final AtomicLong bypassedBytes = new AtomicLong();
    private final AtomicLong droppedReleases = new AtomicLong();
    private final AtomicLong leakedCheckouts = new AtomicLong();
    private final AtomicInteger maxRequested = new AtomicInteger();

    static PoolingHeapByteBufferAllocator forHeap() {
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return new PoolingHeapByteBufferAllocator(Math.max(1L, heapBytes / HEAP_DIVISOR));
    }

    PoolingHeapByteBufferAllocator(long cap) {
        if (cap < 1L) {
            throw new IllegalArgumentException("cap must be at least 1, got: " + cap);
        }
        this.cap = cap;
        List<ConcurrentLinkedQueue<byte[]>> queues = new ArrayList<>(SIZE_CLASSES);
        for (int i = 0; i < SIZE_CLASSES; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
            long classSize = 1L << (MIN_SHIFT + i);
            classEntryLimit[i] = (int) Math.max(1L, Math.min(Integer.MAX_VALUE, (cap / 4) / classSize));
        }
        this.free = List.copyOf(queues);
    }

    @Override
    public ByteBuffer allocate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative, got: " + size);
        }
        expungeStaleCheckouts();
        maxRequested.accumulateAndGet(size, Math::max);
        int shift = targetShift(size);
        byte[] backing;
        if (shift >= 0) {
            allocationsByClass.incrementAndGet(shift - MIN_SHIFT);
            ConcurrentLinkedQueue<byte[]> queue = free.get(shift - MIN_SHIFT);
            byte[] pooled = queue.poll();
            if (pooled != null) {
                freeCounts.decrementAndGet(shift - MIN_SHIFT);
                pooledBytes.addAndGet(-pooled.length);
                poolHits.incrementAndGet();
                backing = pooled;
            } else {
                poolMisses.incrementAndGet();
                backing = new byte[1 << shift];
            }
        } else {
            bypassedAllocations.incrementAndGet();
            bypassedBytes.addAndGet(size);
            backing = new byte[size];
        }
        ByteBuffer buffer = wrap(backing, size);
        byte[] previous = inUse.put(new CheckoutRef(buffer, staleCheckouts), backing);
        if (previous != null) {
            throw new IllegalStateException("checked out a buffer that is already in use");
        }
        return buffer;
    }

    @Override
    public void release(ByteBuffer byteBuffer) {
        expungeStaleCheckouts();
        if (byteBuffer == null) {
            return;
        }
        byte[] backing = inUse.remove(new CheckoutRef(byteBuffer));
        if (backing == null) {
            // Slice, foreign buffer, or stale/double-release of a previous checkout.
            return;
        }
        int shift = pooledShift(backing.length);
        if (shift < 0) {
            return;
        }
        int classIndex = shift - MIN_SHIFT;
        while (true) {
            int count = freeCounts.get(classIndex);
            if (count >= classEntryLimit[classIndex]) {
                droppedReleases.incrementAndGet();
                return;
            }
            if (freeCounts.compareAndSet(classIndex, count, count + 1)) {
                break;
            }
        }
        while (true) {
            long current = pooledBytes.get();
            long next = current + backing.length;
            if (next < 0L || next > cap) {
                freeCounts.decrementAndGet(classIndex);
                droppedReleases.incrementAndGet();
                return;
            }
            if (pooledBytes.compareAndSet(current, next)) {
                free.get(classIndex).offer(backing);
                return;
            }
        }
    }

    /**
     * Drops the tracking entry of every checkout whose {@link ByteBuffer} was collected without a
     * {@link #release}. The backing array is intentionally <em>not</em> re-pooled: the checkout was
     * never released, so its array may still be aliased (e.g. wrapped by a {@code BytesInput} that
     * outlived the buffer); dropping it restores plain GC semantics for the leak instead of risking
     * reuse of a live array. Called on every {@link #allocate}/{@link #release} and by the stats
     * accessors tests read.
     */
    private void expungeStaleCheckouts() {
        Reference<? extends ByteBuffer> stale;
        while ((stale = staleCheckouts.poll()) != null) {
            if (inUse.remove(stale) != null) {
                leakedCheckouts.incrementAndGet();
            }
        }
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    long cap() {
        return cap;
    }

    long pooledBytes() {
        return pooledBytes.get();
    }

    int pooledCount() {
        int count = 0;
        for (ConcurrentLinkedQueue<byte[]> queue : free) {
            count += queue.size();
        }
        return count;
    }

    int checkedOutCount() {
        expungeStaleCheckouts();
        return inUse.size();
    }

    long poolHits() {
        return poolHits.get();
    }

    long poolMisses() {
        return poolMisses.get();
    }

    long bypassedAllocations() {
        return bypassedAllocations.get();
    }

    long bypassedBytes() {
        return bypassedBytes.get();
    }

    long droppedReleases() {
        return droppedReleases.get();
    }

    long leakedCheckouts() {
        expungeStaleCheckouts();
        return leakedCheckouts.get();
    }

    int maxRequested() {
        return maxRequested.get();
    }

    int classEntryLimit(int size) {
        int shift = targetShift(size);
        if (shift < 0) {
            throw new IllegalArgumentException("not a poolable size: " + size);
        }
        return classEntryLimit[shift - MIN_SHIFT];
    }

    /** Allocation counts per size class ({@code arraySize=count}), plus bypasses. For test diagnostics. */
    List<String> allocationHistogram() {
        List<String> histogram = new ArrayList<>();
        for (int i = 0; i < SIZE_CLASSES; i++) {
            long count = allocationsByClass.get(i);
            if (count > 0) {
                histogram.add((1 << (MIN_SHIFT + i)) + "=" + count);
            }
        }
        long bypassed = bypassedAllocations.get();
        if (bypassed > 0) {
            histogram.add("bypassed=" + bypassed + " (" + bypassedBytes.get() + " bytes)");
        }
        return histogram;
    }

    /**
     * Power-of-two shift for a requested size, or {@code -1} when the allocation is too large to
     * pool (caller should allocate the exact size and drop it on release).
     */
    static int targetShift(int requested) {
        if (requested < 0) {
            throw new IllegalArgumentException("size must be non-negative, got: " + requested);
        }
        if (requested == 0) {
            return -1;
        }
        int shift = 32 - Integer.numberOfLeadingZeros(requested - 1);
        if (shift < MIN_SHIFT) {
            return MIN_SHIFT;
        }
        if (shift > MAX_SHIFT) {
            return -1;
        }
        return shift;
    }

    private static int pooledShift(int capacity) {
        if (capacity < MIN_POOLED || capacity > MAX_POOLED) {
            return -1;
        }
        if (Integer.bitCount(capacity) != 1) {
            return -1;
        }
        return Integer.numberOfTrailingZeros(capacity);
    }

    private static ByteBuffer wrap(byte[] backing, int requested) {
        ByteBuffer buffer = ByteBuffer.wrap(backing);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.limit(requested);
        return buffer;
    }

    /**
     * Weak identity reference to a checked-out {@link ByteBuffer}. Reference identity because
     * {@link ByteBuffer#equals} compares contents; weak so an unreleased checkout is GC-purgeable
     * (see {@link #expungeStaleCheckouts()}). The identity hash is captured eagerly so the key
     * stays locatable in the map after the referent is cleared. Lookup keys (the single-argument
     * constructor) are never enqueued.
     */
    private static final class CheckoutRef extends WeakReference<ByteBuffer> {
        private final int identityHash;

        CheckoutRef(ByteBuffer buffer, ReferenceQueue<ByteBuffer> queue) {
            super(buffer, queue);
            this.identityHash = System.identityHashCode(buffer);
        }

        CheckoutRef(ByteBuffer buffer) {
            super(buffer);
            this.identityHash = System.identityHashCode(buffer);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true; // lets a cleared reference polled off the queue remove its own entry
            }
            if (obj instanceof CheckoutRef other) {
                ByteBuffer buffer = get();
                return buffer != null && buffer == other.get();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return identityHash;
        }
    }
}
