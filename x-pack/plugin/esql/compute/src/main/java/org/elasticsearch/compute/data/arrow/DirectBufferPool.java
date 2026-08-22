/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cross-query pool of reusable {@link ArrowBuf}s.
 * <p>
 * Returning a buffer to the OS ({@link ArrowBuf#close()}) is what feeds glibc arena
 * retention: the allocator balance goes to zero, RSS does not. Parking a bounded number of
 * live buffers on the node-level {@link org.elasticsearch.compute.data.BlockFactory} avoids
 * that malloc/free churn after warmup. Pooled buffers stay charged to the REQUEST breaker
 * until {@link #releaseIdle()} or {@link #close()}; that residency is the cost of stable RSS.
 * <p>
 * Undersized idle buffers are kept until the idle cap forces an eviction. Closing them on
 * borrow would be another glibc hand-back. On overflow, the heuristic keeps the larger of
 * (incoming, oldest idle).
 * <p>
 * Thread-safe: several queries may borrow and return concurrently from a shared factory.
 * {@link ConcurrentLinkedDeque} is lock-free; {@link #MAX_POOLED} is enforced with CAS on
 * occupancy, not {@code deque.size()} (O(n)).
 */
public final class DirectBufferPool implements Releasable {

    private static final Logger logger = LogManager.getLogger(DirectBufferPool.class);

    /**
     * Caps <em>idle</em> buffers, not in-flight ones. Each live column reader holds one
     * borrowed buffer that does not count here.
     * <p>
     * 32 is a REQUEST-breaker / RSS bound, not a concurrency proof. The original guess was
     * two overlapping 16-column scans; more concurrent queries allocate extra, then evict
     * on return (prefer larger). Raising this parks more RSS on the breaker; lowering it
     * reintroduces glibc churn on overflow close. Not 64 (or 128) until we measure parked
     * breaker vs concurrent query width — doubling the cap doubles worst-case residency
     * without changing correctness.
     */
    public static final int MAX_POOLED = 32;

    private final ConcurrentLinkedDeque<ArrowBuf> pool = new ConcurrentLinkedDeque<>();
    private final AtomicInteger pooledCount = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Returns a buffer with {@code capacity >= minCapacity}. Walks idle buffers newest-first
     * and puts undersized ones back immediately (occupancy unchanged). Allocates only when
     * nothing idle fits. {@code inspected} bounds the walk so a single undersized buffer
     * cannot {@code pollLast}/{@code offerFirst} forever.
     */
    public ArrowBuf borrow(BufferAllocator allocator, int minCapacity) {
        Objects.requireNonNull(allocator, "allocator");
        if (minCapacity <= 0) {
            throw new IllegalArgumentException("minCapacity must be positive [" + minCapacity + "]");
        }
        if (closed.get()) {
            return allocator.buffer(minCapacity);
        }
        int inspected = 0;
        ArrowBuf buf;
        while (inspected < MAX_POOLED && (buf = pool.pollLast()) != null) {
            inspected++;
            if (closed.get()) {
                pooledCount.decrementAndGet();
                buf.close();
                return allocator.buffer(minCapacity);
            }
            if (buf.capacity() >= minCapacity) {
                pooledCount.decrementAndGet();
                return buf;
            }
            pool.offerFirst(buf);
            if (closed.get() && pool.remove(buf)) {
                pooledCount.decrementAndGet();
                buf.close();
                return allocator.buffer(minCapacity);
            }
        }
        return allocator.buffer(minCapacity);
    }

    /**
     * Returns {@code buf} to the pool, or closes it when the pool is shut, or evicts an older
     * smaller idle buffer when full. Not idempotent: the caller transfers exclusive ownership
     * and must not return the same buffer twice.
     */
    public void returnBuf(ArrowBuf buf) {
        while (buf != null) {
            if (closed.get()) {
                buf.close();
                return;
            }
            int c = pooledCount.get();
            if (c >= MAX_POOLED) {
                buf = evictVictimOrKeep(buf);
                continue;
            }
            if (pooledCount.compareAndSet(c, c + 1)) {
                pool.offerLast(buf);
                if (closed.get() && pool.remove(buf)) {
                    pooledCount.decrementAndGet();
                    buf.close();
                }
                return;
            }
        }
    }

    /**
     * Oldest idle vs incoming: keep the larger, close the smaller. {@code null} means incoming
     * was parked or closed; non-null is the buffer that still needs a slot (retry).
     * <p>
     * O(1) under contention — no scan of the deque. Oldest is least-recently-returned (LIFO
     * borrow / FIFO victim). Capacity tie keeps incoming (fresher).
     */
    private ArrowBuf evictVictimOrKeep(ArrowBuf incoming) {
        ArrowBuf victim = pool.pollFirst();
        if (victim == null) {
            incoming.close();
            return null;
        }
        pooledCount.decrementAndGet();
        if (incoming.capacity() >= victim.capacity()) {
            victim.close();
            return incoming;
        }
        incoming.close();
        return victim;
    }

    /**
     * Closes every idle buffer. The pool stays open for later borrow/return.
     * Production never calls this — parked buffers are the glibc-RSS workaround.
     * Tests call it before asserting the REQUEST breaker and allocator returned to baseline.
     */
    public void releaseIdle() {
        drain();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        drain();
    }

    private void drain() {
        ArrowBuf buf;
        while ((buf = pool.poll()) != null) {
            pooledCount.decrementAndGet();
            try {
                buf.close();
            } catch (RuntimeException e) {
                logger.error("Error closing pooled ArrowBuf", e);
            }
        }
    }
}
