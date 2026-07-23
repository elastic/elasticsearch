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
 * Bounds the memory held by in-flight cache-fill reads on the receive side. A fetched range occupies heap — untracked by circuit
 * breakers — from the moment its bytes arrive (a pooled Netty buffer for transport reads, SDK buffers for object store reads) until a
 * fill thread writes it to the cache file. The fill pool is disk-bound, so without a bound here the network can outrun disk writes and
 * exhaust the heap.
 *
 * This is the receive-side counterpart of {@link org.elasticsearch.xpack.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure},
 * which protects the serving (indexing) node but releases its budget once bytes are sent over the wire — exactly when the receiving
 * node's exposure begins.
 *
 * Unlike the serving side, acquirers here are not rejected when the budget is exhausted: they queue and are granted in FIFO order as
 * budget is released. Rejection would fail warming and prefetching precisely in the overload scenario this exists for; queuing instead
 * slows intake to the pace of the disk writes that release the budget. Callers on latency-sensitive paths (cache-miss reads serving
 * searches) must not wait and should bypass this pressure entirely; see {@link CacheBlobReaderService}.
 *
 * Since grants are strictly FIFO, a queue head that has not been granted for {@link #STALL_WARN_THRESHOLD} means no budget at all was
 * released in that period — typically an in-flight read whose stream was never drained or closed. That state is otherwise silent (the
 * fills just never happen), so it is reported with a WARN at most once per threshold period.
 *
 * There is deliberately no shutdown handling: listeners passed to {@link #acquire} must tolerate never being completed if the node
 * shuts down while they are queued. All acquirers are speculative fills, for which this is inherent anyway.
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
     * when the read no longer occupies heap (the fill wrote it to disk, or the read failed). Completed inline if budget is available
     * and no earlier acquirer is waiting; otherwise queued and completed on {@code executor}, in FIFO order, as budget frees up.
     * Callers must pass the executor on which the deferred read is allowed to run — typically the pool the acquiring thread belongs
     * to — because whatever work follows the grant runs there. A request larger than the whole limit is granted once nothing else is
     * in flight, so it cannot wait forever.
     */
    public void acquire(long bytes, Executor executor, ActionListener<Releasable> listener) {
        assert bytes > 0 : "acquiring [" + bytes + "] bytes";
        final boolean queued;
        final boolean scheduleStallCheck;
        synchronized (mutex) {
            // grant only if no one is already waiting, else a large head-of-queue waiter could starve
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
        // an oversized request is admitted when nothing else is in flight; the budget just goes transiently negative-headroom
        return currentBytes + bytes <= fillBytesLimit || currentBytes == 0;
    }

    // caller must hold mutex
    private void grant(long bytes) {
        currentBytes += bytes;
        metricCurrentBytes.add(bytes);
    }

    private Releasable releasableFor(long bytes) {
        // releaseOnce makes a double-release harmless in production, where it would otherwise silently inflate the budget;
        // assertOnce still surfaces the offending caller in tests
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
        // complete off-mutex and forked: a synchronously failing read would otherwise release (and drain) recursively
        for (Waiter waiter : granted) {
            try {
                waiter.executor().execute(() -> waiter.listener().onResponse(releasableFor(waiter.bytes())));
            } catch (Exception e) {
                // the executor rejected the grant (node shutting down): return the budget and fail the waiter
                release(waiter.bytes());
                waiter.listener().onFailure(e);
            }
        }
    }

    private void scheduleStallCheck(TimeValue delay) {
        try {
            threadPool.schedule(this::checkForStalledHeadWaiter, delay, threadPool.generic());
        } catch (Exception e) {
            // scheduler rejected the task (node shutting down): stall monitoring ends here
            synchronized (mutex) {
                stallCheckScheduled = false;
            }
        }
    }

    /**
     * Runs {@link #STALL_WARN_THRESHOLD} after the queue becomes non-empty and re-arms itself while it stays non-empty, so a WARN is
     * emitted at most once per threshold period. Watching only the head is sufficient: grants are FIFO, so a head older than the
     * threshold means nothing at all was granted in that period.
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
