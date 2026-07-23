/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
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
        final boolean scheduleStallCheck;
        synchronized (mutex) {
            // don't overtake a waiter — a large queue head could otherwise starve
            if (waiters.isEmpty() && fits(bytes)) {
                grant(bytes);
                queued = false;
                scheduleStallCheck = false;
            } else {
                waiters.addLast(new Waiter(bytes, executor, listener, threadPool.relativeTimeInMillis()));
                waitingBytes += bytes;
                metricWaitingBytes.add(bytes);
                logger.trace(() -> Strings.format("queued fill read of [%d] bytes behind [%d] waiters", bytes, waiters.size() - 1));
                queued = true;
                scheduleStallCheck = stallCheckScheduled == false;
                stallCheckScheduled = true;
            }
        }
        if (queued) {
            if (scheduleStallCheck) {
                scheduleStallCheck(stallWarnThreshold);
            }
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
        // off-mutex + forked: a synchronously-failing read would otherwise recurse into release
        for (Waiter waiter : granted) {
            try {
                waiter.executor().execute(() -> waiter.listener().onResponse(releasableFor(waiter.bytes())));
            } catch (Exception e) {
                // executor rejected (node shutting down): return budget, fail waiter
                release(waiter.bytes());
                waiter.listener().onFailure(e);
            }
        }
    }

    private void scheduleStallCheck(TimeValue delay) {
        try {
            threadPool.schedule(this::checkForStalledHeadWaiter, delay, threadPool.generic());
        } catch (Exception e) {
            // scheduler rejected (node shutting down): stall monitoring ends
            synchronized (mutex) {
                stallCheckScheduled = false;
            }
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
            scheduleStallCheck(stallWarnThreshold);
        } else {
            scheduleStallCheck(TimeValue.timeValueMillis(stallWarnThreshold.millis() - headWaitedMillis));
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
