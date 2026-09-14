/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

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
 * <p>Each {@link #allocate} wraps a fresh {@link ByteBuffer} around a pooled (or new) {@code byte[]}.
 * Identities are not recycled, so a stale {@link #release} of a previous checkout cannot re-pool an
 * array another caller still holds. A slice or foreign buffer is ignored; parquet-mr must
 * {@link #release} the instance {@link #allocate} returned. Checkouts are held strongly until that
 * release, so a missed one pins the array for the life of this node-wide pool instead of leaving
 * it to GC.
 */
final class PoolingHeapByteBufferAllocator implements ByteBufferAllocator {

    /**
     * Idle pooled bytes cap as a fraction of max heap. Smaller than {@link ParquetIoWatermark}'s
     * {@code heap / 8} because those I/O bytes are request-breaker charged and this idle set is not.
     * {@code / 32} on a default ~4 GB heap is 128 MB — enough to reuse typical footer/dictionary
     * copies across hundreds of files, small enough that it cannot recreate the ~3.8 GB parent trip.
     */
    static final int HEAP_DIVISOR = 32;

    /** Smallest power-of-two size class. Below this, allocate this size and still pool. */
    static final int MIN_POOLED = 1 << 8;

    /**
     * Largest array that may enter the free list. Larger allocations (rare) are exact-sized and
     * dropped on release so one footer cannot occupy the whole cap.
     */
    static final int MAX_POOLED = 1 << 20;

    private static final int MIN_SHIFT = Integer.numberOfTrailingZeros(MIN_POOLED);
    private static final int MAX_SHIFT = Integer.numberOfTrailingZeros(MAX_POOLED);
    private static final int SIZE_CLASSES = MAX_SHIFT - MIN_SHIFT + 1;

    private final long cap;
    private final AtomicLong pooledBytes = new AtomicLong();
    private final List<ConcurrentLinkedQueue<byte[]>> free;
    /**
     * Identity of every {@link ByteBuffer} currently checked out, mapped to its backing array.
     * Equality is reference identity because {@link ByteBuffer#equals} compares contents. Strong
     * keys: a checkout parquet-mr never {@link #release}s stays reachable until this allocator
     * itself is collected.
     */
    private final ConcurrentHashMap<Identity, byte[]> inUse = new ConcurrentHashMap<>();

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
        }
        this.free = List.copyOf(queues);
    }

    @Override
    public ByteBuffer allocate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative, got: " + size);
        }
        int shift = targetShift(size);
        byte[] backing;
        if (shift >= 0) {
            ConcurrentLinkedQueue<byte[]> queue = free.get(shift - MIN_SHIFT);
            byte[] pooled = queue.poll();
            if (pooled != null) {
                pooledBytes.addAndGet(-pooled.length);
                backing = pooled;
            } else {
                backing = new byte[1 << shift];
            }
        } else {
            backing = new byte[size];
        }
        ByteBuffer buffer = wrap(backing, size);
        byte[] previous = inUse.put(new Identity(buffer), backing);
        if (previous != null) {
            throw new IllegalStateException("checked out a buffer that is already in use");
        }
        return buffer;
    }

    @Override
    public void release(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return;
        }
        byte[] backing = inUse.remove(new Identity(byteBuffer));
        if (backing == null) {
            // Slice, foreign buffer, or stale/double-release of a previous checkout.
            return;
        }
        int shift = pooledShift(backing.length);
        if (shift < 0) {
            return;
        }
        while (true) {
            long current = pooledBytes.get();
            long next = current + backing.length;
            if (next < 0L || next > cap) {
                return;
            }
            if (pooledBytes.compareAndSet(current, next)) {
                free.get(shift - MIN_SHIFT).offer(backing);
                return;
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
        return inUse.size();
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

    private static final class Identity {
        private final ByteBuffer buffer;

        Identity(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Identity other && other.buffer == buffer;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(buffer);
        }
    }
}
