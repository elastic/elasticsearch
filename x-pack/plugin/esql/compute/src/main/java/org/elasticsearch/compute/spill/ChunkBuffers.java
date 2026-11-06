/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The fixed set of chunk buffers one codec task serializes into: charged to the query once, up front, so that a task that
 * started never has to allocate under pressure, and returned when the task is done. The depth bounds the serialized bytes
 * a task can have in flight, so a withheld completion stalls the task and never grows its memory.
 * <p>
 * {@link #acquire} may wait, and only the codec thread calls it; {@link #release} may come from any thread, including a
 * backend's completion thread. Cancellation, a failure and an interrupt wake a waiting acquire; a sink that never
 * completes is left behind only by cancellation, so whoever shuts the codec pool down cancels its tasks first.
 */
final class ChunkBuffers implements Releasable {

    private final CircuitBreaker breaker;
    private final long chargedBytes;
    private final int chunkSize;
    private final ArrayDeque<byte[]> free;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();
    private boolean cancelled;
    private Exception failure;
    private boolean closed;

    /**
     * @throws CircuitBreakingException when the buffers cannot be charged; nothing is allocated then
     */
    ChunkBuffers(CircuitBreaker breaker, int chunkSize, int depth) throws CircuitBreakingException {
        if (chunkSize <= 0 || depth <= 0) {
            throw new IllegalArgumentException("chunk size [" + chunkSize + "] and depth [" + depth + "] must be positive");
        }
        this.breaker = breaker;
        this.chunkSize = chunkSize;
        this.chargedBytes = (long) chunkSize * depth;
        breaker.addEstimateBytesAndMaybeBreak(chargedBytes, "spill chunk buffers");
        this.free = new ArrayDeque<>(depth);
        for (int i = 0; i < depth; i++) {
            free.add(new byte[chunkSize]);
        }
    }

    int chunkSize() {
        return chunkSize;
    }

    /**
     * A free buffer, waiting for one if all are out. Throws when the task was cancelled or an earlier append failed, so
     * the codec thread leaves its serialization loop at the next chunk boundary.
     */
    byte[] acquire() throws IOException {
        lock.lock();
        try {
            while (true) {
                if (failure != null) {
                    throw new IOException("spill append failed", failure);
                }
                if (cancelled) {
                    throw new TaskCancelledException("spill cancelled");
                }
                byte[] buffer = free.poll();
                if (buffer != null) {
                    return buffer;
                }
                try {
                    available.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskCancelledException("spill interrupted");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void release(byte[] buffer) {
        lock.lock();
        try {
            if (closed == false) {
                free.add(buffer);
            }
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void cancel() {
        lock.lock();
        try {
            cancelled = true;
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void fail(Exception e) {
        lock.lock();
        try {
            if (failure == null) {
                failure = e;
            }
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    boolean isCancelled() {
        lock.lock();
        try {
            return cancelled;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the charge. Buffers still out with a sink are dropped when that sink releases them; the charge does not wait
     * for them, because it covered the buffers' existence for the task, not for the sink.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            free.clear();
        } finally {
            lock.unlock();
        }
        breaker.addWithoutBreaking(-chargedBytes);
    }
}
