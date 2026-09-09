/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates wall-clock and CPU time for external data-source reads at the operator level.
 *
 * <p>Two measurement modes:
 * <ul>
 *   <li>{@link #recordOnThread} — for measurements taken on the <em>owner thread</em>
 *       (the drain thread / producerExecutor). Both wall and CPU are always accumulated.
 *   <li>{@link #record} — for measurements from any thread. Wall time is always
 *       accumulated when {@code startNanos >= 0}. CPU is accumulated only when the calling
 *       thread is not the owner thread, preventing double-counting when degenerate
 *       (inline/direct) executors collapse the open and drain phases onto one thread.
 *       Pass {@code startNanos = -1} to skip the wall contribution (parallel workers).
 * </ul>
 *
 * <p>The owner thread is established with {@link #initOwnerThread()} from the drain loop
 * before the first {@link #recordOnThread} call. {@link #initOwnerThread()} is called on
 * every drain-loop entry (including re-entries on potentially different pool threads), so
 * {@code ownerThread} always reflects the thread currently executing the drain.
 *
 * <p>Both methods accept start timestamps and compute the delta internally, so callers
 * only capture {@code System.nanoTime()} / {@code ThreadCpuTimer.currentNanos()} once.
 */
public final class ExternalReadCounters {

    private volatile Thread ownerThread;
    private final AtomicLong readNanosAcc = new AtomicLong();
    private final AtomicLong readCpuNanosAcc = new AtomicLong();

    /**
     * Establishes the current thread as the owner (drain) thread. Call at the start of every
     * drain-loop entry, before the first {@link #recordOnThread} call in that entry. Safe to
     * call multiple times — re-entries on different pool threads update the owner accordingly.
     */
    public void initOwnerThread() {
        ownerThread = Thread.currentThread();
    }

    /**
     * Records time measured on the drain thread. Both wall and CPU deltas are unconditionally
     * accumulated.
     *
     * @param startNanos    value of {@code System.nanoTime()} before the measured work
     * @param startCpuNanos value of {@code ThreadCpuTimer.currentNanos()} before the work;
     *                      pass {@code -1} (unsupported) to skip CPU accumulation
     */
    public void recordOnThread(long startNanos, long startCpuNanos) {
        assert Thread.currentThread() == ownerThread
            : "recordOnThread called from " + Thread.currentThread() + " but owner is " + ownerThread;
        readNanosAcc.addAndGet(System.nanoTime() - startNanos);
        if (startCpuNanos >= 0) {
            readCpuNanosAcc.addAndGet(ThreadCpuTimer.elapsedNanos(startCpuNanos));
        }
    }

    /**
     * Records time from any thread context. Intended for the open phase (executor thread) and
     * parallel worker threads (PPC/SPPC).
     * <ul>
     *   <li>Wall: added when {@code startNanos >= 0}; pass {@code -1} for parallel workers
     *       that run concurrently with the drain loop (their wall is already implicit in the
     *       drain thread's blocking wait).
     *   <li>CPU: added only when the calling thread is not the owner thread. When they are
     *       equal the drain's {@link #recordOnThread} already captured the CPU, avoiding
     *       double-counting on degenerate (inline) executors.
     * </ul>
     *
     * @param startNanos    start timestamp for wall, or {@code -1} to skip wall contribution
     * @param startCpuNanos value of {@code ThreadCpuTimer.currentNanos()} before the work;
     *                      pass {@code -1} (unsupported) to skip CPU accumulation
     */
    public void record(long startNanos, long startCpuNanos) {
        if (startNanos >= 0) {
            readNanosAcc.addAndGet(System.nanoTime() - startNanos);
        }
        if (startCpuNanos >= 0 && Thread.currentThread() != ownerThread) {
            readCpuNanosAcc.addAndGet(ThreadCpuTimer.elapsedNanos(startCpuNanos));
        }
    }

    public long readNanos() {
        return readNanosAcc.get();
    }

    public long readCpuNanos() {
        return readCpuNanosAcc.get();
    }

    /** Directly adds to both accumulators. For use in tests only. */
    void add(long readNanos, long readCpuNanos) {
        readNanosAcc.addAndGet(readNanos);
        readCpuNanosAcc.addAndGet(readCpuNanos);
    }
}
