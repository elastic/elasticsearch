/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Bounds heap held by in-flight cache-fill reads on the receive side. A fetched range occupies untracked heap (pooled Netty buffer /
 * SDK buffers) from network arrival until a fill thread writes it to disk; without a bound the network outruns the disk-bound fill
 * pool and exhausts heap.
 *
 * Receive-side counterpart of {@link org.elasticsearch.xpack.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure}, which
 * releases at send time — exactly when receiver exposure starts.
 *
 * Acquirers queue FIFO rather than being rejected: rejection would fail warming/prefetching in the very overload this exists for.
 * Latency-sensitive paths (cache-miss reads) must bypass; see {@link CacheBlobReaderService}.
 *
 * A queue head unmoved for {@link #STALL_WARN_THRESHOLD} means nothing was released in that period — typically an admitted read whose
 * stream was never drained or closed. WARN'd at most once per period.
 *
 * No shutdown handling: queued listeners must tolerate never completing (all acquirers are speculative fills, for which this is
 * inherent anyway).
 */
public class FillCacheMemoryPressure {

    public static final String CURRENT_BYTES_METRIC = "es.fill_cache.memory.current";
    public static final String WAITING_BYTES_METRIC = "es.fill_cache.memory.waiting.current";

    public static final Setting<ByteSizeValue> FILL_BYTES_LIMIT = Setting.memorySizeSetting(
        "stateless.fill_cache.memory.limit",
        "10%",
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> STALL_WARN_THRESHOLD = Setting.timeSetting(
        "stateless.fill_cache.memory.stall_warn_threshold",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(FillCacheMemoryPressure.class);

    private final long fillBytesLimit;
    private final TimeValue stallWarnThreshold;
    private final ThreadPool threadPool;
    private final LongUpDownCounter metricCurrentBytes;
    private final LongUpDownCounter metricWaitingBytes;

    private final Object mutex = new Object();
    // all guarded by mutex
    private long currentBytes = 0;
    private long waitingBytes = 0;
    private boolean stallCheckScheduled = false;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    private record Waiter(long bytes, Executor executor, ActionListener<Releasable> listener, long enqueuedAtMillis) {}

    public FillCacheMemoryPressure(Settings settings, MeterRegistry meterRegistry, ThreadPool threadPool) {
        this.fillBytesLimit = FILL_BYTES_LIMIT.get(settings).getBytes();
        this.stallWarnThreshold = STALL_WARN_THRESHOLD.get(settings);
        this.threadPool = threadPool;
        this.metricCurrentBytes = meterRegistry.registerLongUpDownCounter(
            CURRENT_BYTES_METRIC,
            "Current bytes admitted for in-flight cache-fill reads",
            "bytes"
        );
        this.metricWaitingBytes = meterRegistry.registerLongUpDownCounter(
            WAITING_BYTES_METRIC,
            "Bytes of cache-fill reads waiting for memory budget",
            "bytes"
        );
    }

    /**
     * Acquires {@code bytes} of fill budget. The listener is completed with a {@link Releasable} that must be released exactly once,
     * when the read no longer occupies heap. Completed inline if budget is free and no earlier acquirer is waiting; otherwise queued
     * FIFO and completed on {@code executor} — must be the pool the deferred read is allowed to run on (typically the acquirer's own
     * pool). Requests larger than the whole limit are granted once nothing else is in flight, so they cannot wait forever.
     */
    public void acquire(long bytes, Executor executor, ActionListener<Releasable> listener) {
        assert bytes > 0 : "acquiring [" + bytes + "] bytes";
        final boolean queued;
        synchronized (mutex) {
            // don't overtake a waiter — a large queue head could otherwise starve
            if (waiters.isEmpty() && fits(bytes)) {
                grant(bytes);
                queued = false;
            } else {
                waiters.addLast(new Waiter(bytes, executor, listener, threadPool.relativeTimeInMillis()));
                waitingBytes += bytes;
                metricWaitingBytes.add(bytes);
                logger.trace(() -> Strings.format("queued fill read of [%d] bytes behind [%d] waiters", bytes, waiters.size() - 1));
                queued = true;
                // schedule under mutex so a schedule failure atomically leaves stallCheckScheduled=false;
                // otherwise a race window between failure and flag-flip could leave the queue silently unmonitored
                if (stallCheckScheduled == false) {
                    stallCheckScheduled = tryScheduleStallCheckLocked(stallWarnThreshold);
                }
            }
        }
        if (queued) {
            return;
        }
        listener.onResponse(releasableFor(bytes));
    }

    // caller must hold mutex
    private boolean fits(long bytes) {
        // oversized request admitted when idle; budget goes transiently over-limit
        return currentBytes + bytes <= fillBytesLimit || currentBytes == 0;
    }

    // caller must hold mutex
    private void grant(long bytes) {
        currentBytes += bytes;
        metricCurrentBytes.add(bytes);
    }

    private Releasable releasableFor(long bytes) {
        // releaseOnce: harmless double-release in prod; assertOnce still surfaces the caller in tests
        return Releasables.assertOnce(Releasables.releaseOnce(() -> release(bytes)));
    }

    private void release(long bytes) {
        final List<Exception> listenerFailures = new ArrayList<>();
        // Iterative rather than recursive: each pass returns budget and admits newly-fitting waiters; a grant whose
        // executor rejects it (node shutting down) has its bytes reclaimed on the next pass. Grants are delivered
        // off-mutex and forked, so a synchronously-failing read cannot re-enter this method on the same stack.
        long bytesToReturn = bytes;
        while (bytesToReturn > 0) {
            long reclaimed = 0;
            for (Waiter waiter : returnBudgetAndGrantWaiters(bytesToReturn)) {
                try {
                    waiter.executor().execute(() -> deliverGrant(waiter));
                } catch (Exception e) {
                    // executor rejected (node shutting down): reclaim budget, fail waiter — but collect (do not
                    // propagate) any exception from onFailure so subsequent granted waiters are still notified.
                    // Mirrors ActionListener.onFailure(Iterable, Exception) at server/action/ActionListener.java:319.
                    reclaimed += waiter.bytes();
                    try {
                        waiter.listener().onFailure(e);
                    } catch (Exception listenerException) {
                        listenerFailures.add(listenerException);
                    }
                }
            }
            bytesToReturn = reclaimed;
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(listenerFailures);
    }

    // returns {@code bytes} to the budget and grants waiters, FIFO, while the head fits; the caller must complete
    // the returned waiters' listeners without holding the mutex
    private List<Waiter> returnBudgetAndGrantWaiters(long bytes) {
        final List<Waiter> granted = new ArrayList<>();
        synchronized (mutex) {
            currentBytes -= bytes;
            metricCurrentBytes.add(-bytes);
            assert currentBytes >= 0 : "fill budget underflow [" + currentBytes + "]";
            Waiter head;
            while ((head = waiters.peekFirst()) != null && fits(head.bytes())) {
                waiters.pollFirst();
                waitingBytes -= head.bytes();
                metricWaitingBytes.add(-head.bytes());
                grant(head.bytes());
                granted.add(head);
            }
        }
        return granted;
    }

    // runs on the waiter's executor; hands the Releasable to the listener and releases the budget if the listener throws
    // before it can take ownership (otherwise the grant would leak — currentBytes stays inflated with no live read).
    private void deliverGrant(Waiter waiter) {
        final Releasable budget = releasableFor(waiter.bytes());
        boolean handedOff = false;
        try {
            waiter.listener().onResponse(budget);
            handedOff = true;
        } finally {
            if (handedOff == false) {
                budget.close();
            }
        }
    }

    // caller must hold mutex
    private boolean tryScheduleStallCheckLocked(TimeValue delay) {
        try {
            threadPool.schedule(this::checkForStalledHeadWaiter, delay, threadPool.generic());
            return true;
        } catch (Exception e) {
            // scheduler rejected (node shutting down): stall monitoring ends
            return false;
        }
    }

    /**
     * Runs {@link #STALL_WARN_THRESHOLD} after the queue becomes non-empty and re-arms while it stays non-empty (WARN at most once per
     * period). Watching only the head suffices: FIFO grants mean a stale head implies zero grants in that period.
     */
    private void checkForStalledHeadWaiter() {
        final long headWaitedMillis;
        final long headBytes;
        final int waiterCount;
        final long waitingBytesSnapshot;
        final long currentBytesSnapshot;
        synchronized (mutex) {
            final Waiter head = waiters.peekFirst();
            if (head == null) {
                stallCheckScheduled = false;
                return;
            }
            headWaitedMillis = threadPool.relativeTimeInMillis() - head.enqueuedAtMillis();
            headBytes = head.bytes();
            waiterCount = waiters.size();
            waitingBytesSnapshot = waitingBytes;
            currentBytesSnapshot = currentBytes;
        }
        final TimeValue nextDelay;
        if (headWaitedMillis >= stallWarnThreshold.millis()) {
            logger.warn(
                "cache-fill memory budget stalled: no budget released for [{}] while the queue head waits for [{}] bytes; "
                    + "[{}] waiters totaling [{}] bytes; [{}] of [{}] bytes admitted but not yet released — "
                    + "check for admitted reads whose stream was never drained or closed",
                TimeValue.timeValueMillis(headWaitedMillis),
                headBytes,
                waiterCount,
                waitingBytesSnapshot,
                currentBytesSnapshot,
                fillBytesLimit
            );
            nextDelay = stallWarnThreshold;
        } else {
            nextDelay = TimeValue.timeValueMillis(stallWarnThreshold.millis() - headWaitedMillis);
        }
        synchronized (mutex) {
            // re-check under mutex: the queue may have drained between the snapshot above and here
            if (waiters.isEmpty()) {
                stallCheckScheduled = false;
            } else {
                stallCheckScheduled = tryScheduleStallCheckLocked(nextDelay);
            }
        }
    }

    // exposed for tests
    public long getCurrentBytes() {
        synchronized (mutex) {
            return currentBytes;
        }
    }

    // exposed for tests
    public int getWaiterCount() {
        synchronized (mutex) {
            return waiters.size();
        }
    }
}
