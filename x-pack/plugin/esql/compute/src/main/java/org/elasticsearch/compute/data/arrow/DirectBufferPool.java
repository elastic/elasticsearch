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
 * Thread-safe: several queries may borrow and return concurrently from a shared factory.
 */
public final class DirectBufferPool implements Releasable {

    private static final Logger logger = LogManager.getLogger(DirectBufferPool.class);

    /**
     * Caps idle buffers. 32 covers two concurrent 16-column scans; overflow is freed
     * (falls back to allocate-on-demand).
     */
    public static final int MAX_POOLED = 32;

    private final ConcurrentLinkedDeque<ArrowBuf> pool = new ConcurrentLinkedDeque<>();
    private final AtomicInteger pooledCount = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Returns a buffer with {@code capacity >= minCapacity}. Prefers an idle pooled buffer;
     * undersized idle buffers are closed (the remaining malloc source after warmup).
     */
    public ArrowBuf borrow(BufferAllocator allocator, int minCapacity) {
        Objects.requireNonNull(allocator, "allocator");
        if (minCapacity <= 0) {
            throw new IllegalArgumentException("minCapacity must be positive [" + minCapacity + "]");
        }
        if (closed.get() == false) {
            ArrowBuf buf;
            while ((buf = pool.pollLast()) != null) {
                pooledCount.decrementAndGet();
                if (buf.capacity() >= minCapacity) {
                    return buf;
                }
                buf.close();
            }
        }
        return allocator.buffer(minCapacity);
    }

    /**
     * Returns {@code buf} to the pool, or closes it when the pool is shut, full, or {@code buf}
     * is null. Not idempotent: the caller transfers exclusive ownership and must not return the
     * same buffer twice.
     */
    public void returnBuf(ArrowBuf buf) {
        if (buf == null) {
            return;
        }
        if (closed.get() == false) {
            int c;
            do {
                c = pooledCount.get();
                if (c >= MAX_POOLED || closed.get()) {
                    buf.close();
                    return;
                }
            } while (pooledCount.compareAndSet(c, c + 1) == false);
            pool.offer(buf);
            if (closed.get() && pool.remove(buf)) {
                pooledCount.decrementAndGet();
                buf.close();
            }
            return;
        }
        buf.close();
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
