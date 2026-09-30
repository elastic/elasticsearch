/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Per-node gate that bounds how many ES|QL queries reading an external dataset may run on this coordinator at once.
 * <p>
 * Every dataset query lists the dataset and builds per-file planning state on the coordinator before it reads any
 * data. That state is tens of megabytes for a dataset of a few thousand files, and a dashboard sends one query per
 * panel at the same moment, so it is the number of such queries in flight, not the size of any one, that exhausts
 * the heap. This gate is consulted once per query, after the query is known to name a dataset and before any
 * storage request is made, and the slot is held until the query completes.
 * <p>
 * A query that finds every slot taken waits in a bounded FIFO queue (holding no thread) for up to
 * {@link ExternalSourceSettings#ADMISSION_QUEUE_TIMEOUT}; a query that finds the queue full, or waits out the
 * timeout, is refused with {@link EsRejectedExecutionException}, which the REST layer answers with 429. While any
 * query is waiting, one periodic sweep removes the ones that have timed out or whose task was cancelled. When a
 * queued query is admitted, its continuation is forked onto {@code resumeExecutor} under the thread context it was
 * queued with, so it resumes on the same pool and with the same security context as a query admitted immediately.
 * <p>
 * All three limits are dynamic. Setting the concurrency limit to {@code 0} turns the gate off: queued queries are
 * admitted, and slots handed out while it is off are never counted, even after it is turned back on. So right after
 * turning it back on, queries admitted while it was off may still be running on top of the limit. Lowering a limit
 * below what is running cancels nothing; new arrivals wait or are refused until enough queries finish.
 */
public final class ExternalQueryAdmission {

    private final ThreadPool threadPool;
    private final Executor resumeExecutor;
    private final ThreadContext threadContext;
    /** Set only by {@link #unlimited()}: the gate stays off and ignores setting changes, since it has no pool to queue on. */
    private final boolean alwaysOff;

    /** Guarded by {@code this}. */
    private int maxConcurrentQueries;
    /** Guarded by {@code this}. */
    private int maxQueuedQueries;
    /** Guarded by {@code this}. */
    private TimeValue queueTimeout;

    /** Counted slots currently held. Guarded by {@code this}. */
    private int running;
    /** Guarded by {@code this}. */
    private final ArrayDeque<Waiter> queue = new ArrayDeque<>();
    /** The sweep that expires waiters; scheduled while the queue is non-empty. Guarded by {@code this}. */
    private Scheduler.Cancellable sweep;

    /** Guarded by {@code this}. */
    private long admitted;
    /** Guarded by {@code this}. */
    private long refusedQueueFull;
    /** Guarded by {@code this}. */
    private long refusedTimeout;

    /**
     * A gate that admits every query and counts none, for callers that do not configure admission. It never queues,
     * so it needs no thread pool.
     */
    public static ExternalQueryAdmission unlimited() {
        return new ExternalQueryAdmission();
    }

    private ExternalQueryAdmission() {
        this.threadPool = null;
        this.resumeExecutor = null;
        this.threadContext = null;
        this.alwaysOff = true;
        this.maxConcurrentQueries = 0;
        this.maxQueuedQueries = 0;
        this.queueTimeout = TimeValue.ZERO;
    }

    public ExternalQueryAdmission(
        ThreadPool threadPool,
        Executor resumeExecutor,
        int maxConcurrentQueries,
        int maxQueuedQueries,
        TimeValue queueTimeout
    ) {
        this.threadPool = threadPool;
        this.resumeExecutor = resumeExecutor;
        this.threadContext = threadPool.getThreadContext();
        this.alwaysOff = false;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.maxQueuedQueries = maxQueuedQueries;
        this.queueTimeout = queueTimeout;
    }

    /**
     * Asks for a slot. {@code listener} receives the slot, to be closed exactly once when the query completes
     * (closing it again is a no-op), or an {@link EsRejectedExecutionException} if the node is too busy, or a
     * {@link TaskCancelledException} if {@code cancelled} turns true while the query waits. When a slot is free the
     * listener is completed on the calling thread; otherwise it is completed later on {@code resumeExecutor}.
     */
    public void acquire(BooleanSupplier cancelled, ActionListener<Releasable> listener) {
        final Slot slot;
        final String refusal;
        synchronized (this) {
            if (maxConcurrentQueries == 0) {
                admitted++;
                slot = new Slot(false);
                refusal = null;
            } else if (running < maxConcurrentQueries) {
                // Never true while queries wait: a slot freed with a non-empty queue is handed straight to its head.
                assert queue.isEmpty() : "a slot was free while queries waited";
                running++;
                admitted++;
                slot = new Slot(true);
                refusal = null;
            } else if (queue.size() < maxQueuedQueries && queueTimeout.nanos() > 0) {
                enqueue(cancelled, listener);
                return;
            } else {
                refusedQueueFull++;
                slot = null;
                refusal = queueFullMessage();
            }
        }
        if (slot == null) {
            listener.onFailure(new EsRejectedExecutionException(refusal, false));
        } else {
            listener.onResponse(slot);
        }
    }

    /** Called with the monitor held. */
    private void enqueue(BooleanSupplier cancelled, ActionListener<Releasable> listener) {
        queue.add(
            new Waiter(
                ContextPreservingActionListener.wrapPreservingContext(listener, threadContext),
                cancelled,
                threadPool.relativeTimeInNanos()
            )
        );
        if (sweep == null) {
            TimeValue interval = TimeValue.timeValueMillis(Math.max(1, Math.min(1000, queueTimeout.millis())));
            sweep = threadPool.scheduleWithFixedDelay(this::sweep, interval, threadPool.generic());
        }
    }

    /** Removes waiters that have timed out or whose task was cancelled, and stops itself once nobody is waiting. */
    private void sweep() {
        List<Waiter> expired = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        synchronized (this) {
            long now = threadPool.relativeTimeInNanos();
            for (Iterator<Waiter> it = queue.iterator(); it.hasNext();) {
                Waiter waiter = it.next();
                long waitedNanos = now - waiter.queuedAtNanos;
                Exception failure;
                if (waiter.cancelled.getAsBoolean()) {
                    failure = new TaskCancelledException("cancelled while waiting for a dataset query slot");
                } else if (waitedNanos >= queueTimeout.nanos()) {
                    refusedTimeout++;
                    failure = new EsRejectedExecutionException(timeoutMessage(TimeValue.timeValueNanos(waitedNanos)), false);
                } else {
                    continue;
                }
                it.remove();
                expired.add(waiter);
                failures.add(failure);
            }
            stopSweepIfIdleLocked();
        }
        for (int i = 0; i < expired.size(); i++) {
            expired.get(i).listener.onFailure(failures.get(i));
        }
    }

    /** Called with the monitor held. */
    private void stopSweepIfIdleLocked() {
        if (queue.isEmpty() && sweep != null) {
            sweep.cancel();
            sweep = null;
        }
    }

    /** Returns a counted slot and admits as many queued queries as the limit now allows. */
    private void release() {
        List<Waiter> promoted;
        synchronized (this) {
            running--;
            assert running >= 0 : "released more dataset query slots than were handed out";
            promoted = promoteLocked();
        }
        resume(promoted);
    }

    /** Called with the monitor held. Admits queued queries while slots are free, or all of them when the gate is off. */
    private List<Waiter> promoteLocked() {
        List<Waiter> promoted = new ArrayList<>();
        while (queue.isEmpty() == false && (maxConcurrentQueries == 0 || running < maxConcurrentQueries)) {
            Waiter waiter = queue.poll();
            boolean counted = maxConcurrentQueries != 0;
            if (counted) {
                running++;
            }
            admitted++;
            waiter.counted = counted;
            promoted.add(waiter);
        }
        stopSweepIfIdleLocked();
        return promoted;
    }

    private void resume(List<Waiter> promoted) {
        for (Waiter waiter : promoted) {
            Slot slot = new Slot(waiter.counted);
            try {
                resumeExecutor.execute(() -> waiter.listener.onResponse(slot));
            } catch (Exception e) {
                slot.close();
                waiter.listener.onFailure(e);
            }
        }
    }

    public void setMaxConcurrentQueries(int maxConcurrentQueries) {
        if (alwaysOff) {
            return;
        }
        List<Waiter> promoted;
        synchronized (this) {
            this.maxConcurrentQueries = maxConcurrentQueries;
            promoted = promoteLocked();
        }
        resume(promoted);
    }

    public synchronized void setMaxQueuedQueries(int maxQueuedQueries) {
        if (alwaysOff == false) {
            this.maxQueuedQueries = maxQueuedQueries;
        }
    }

    public synchronized void setQueueTimeout(TimeValue queueTimeout) {
        if (alwaysOff == false) {
            this.queueTimeout = queueTimeout;
        }
    }

    /** Called with the monitor held. */
    private String queueFullMessage() {
        return "too many concurrent ES|QL dataset queries on this node: ["
            + running
            + "] running with a limit of ["
            + maxConcurrentQueries
            + "] (set by ["
            + ExternalSourceSettings.ADMISSION_MAX_CONCURRENT_QUERIES.getKey()
            + "]), and ["
            + queue.size()
            + "] waiting with a limit of ["
            + maxQueuedQueries
            + "] (set by ["
            + ExternalSourceSettings.ADMISSION_MAX_QUEUED_QUERIES.getKey()
            + "]); retry the query later";
    }

    /** Called with the monitor held. */
    private String timeoutMessage(TimeValue waited) {
        return "too many concurrent ES|QL dataset queries on this node: waited ["
            + waited
            + "] for one of ["
            + maxConcurrentQueries
            + "] slots (set by ["
            + ExternalSourceSettings.ADMISSION_MAX_CONCURRENT_QUERIES.getKey()
            + "]); retry the query later";
    }

    /** Counted slots currently held. */
    public synchronized int running() {
        return running;
    }

    /** Queries currently waiting for a slot. */
    public synchronized int queued() {
        return queue.size();
    }

    /** Queries admitted since the node started, immediately or after waiting. */
    public synchronized long admitted() {
        return admitted;
    }

    /** Queries refused because the queue was full. */
    public synchronized long refusedQueueFull() {
        return refusedQueueFull;
    }

    /** Queries refused because they waited longer than the queue timeout. */
    public synchronized long refusedTimeout() {
        return refusedTimeout;
    }

    public static final String RUNNING_CURRENT = "es.esql.datasources.admission.running.current";
    public static final String QUEUED_CURRENT = "es.esql.datasources.admission.queued.current";
    public static final String ADMITTED_TOTAL = "es.esql.datasources.admission.admitted.total";
    public static final String REFUSED_TOTAL = "es.esql.datasources.admission.refused.total";
    static final String REASON_ATTRIBUTE = "es_datasource_reason";

    /**
     * Publishes the gate's counts, so an operator can see dataset queries waiting or being refused, and a slot that
     * was never given back shows up as a running count that does not return to zero on an idle node.
     */
    public void registerMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongAsyncGauge(
            RUNNING_CURRENT,
            "ES|QL dataset queries holding an admission slot on this node",
            "unit",
            () -> new LongWithAttributes(running())
        );
        meterRegistry.registerLongAsyncGauge(
            QUEUED_CURRENT,
            "ES|QL dataset queries waiting for an admission slot on this node",
            "unit",
            () -> new LongWithAttributes(queued())
        );
        meterRegistry.registerLongAsyncCounter(
            ADMITTED_TOTAL,
            "ES|QL dataset queries admitted on this node",
            "unit",
            () -> new LongWithAttributes(admitted())
        );
        meterRegistry.registerLongsAsyncCounter(
            REFUSED_TOTAL,
            "ES|QL dataset queries refused on this node because every slot was taken, by reason",
            "unit",
            () -> List.of(
                new LongWithAttributes(refusedQueueFull(), Map.of(REASON_ATTRIBUTE, "queue_full")),
                new LongWithAttributes(refusedTimeout(), Map.of(REASON_ATTRIBUTE, "timeout"))
            )
        );
    }

    /** Test-only: whether the sweep that expires waiters is scheduled. */
    synchronized boolean sweepScheduled() {
        return sweep != null;
    }

    private static final class Waiter {
        final ActionListener<Releasable> listener;
        final BooleanSupplier cancelled;
        final long queuedAtNanos;
        /** Written under the gate's monitor before the waiter is resumed. */
        boolean counted;

        Waiter(ActionListener<Releasable> listener, BooleanSupplier cancelled, long queuedAtNanos) {
            this.listener = listener;
            this.cancelled = cancelled;
            this.queuedAtNanos = queuedAtNanos;
        }
    }

    /** A held slot. Closing it more than once is a no-op, so both arms of a listener may close it. */
    private final class Slot implements Releasable {
        private final boolean counted;
        private final AtomicBoolean closed = new AtomicBoolean();

        Slot(boolean counted) {
            this.counted = counted;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) && counted) {
                release();
            }
        }
    }
}
