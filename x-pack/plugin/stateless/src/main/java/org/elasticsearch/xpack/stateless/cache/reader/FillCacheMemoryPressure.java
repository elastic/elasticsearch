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
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

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
 */
public class FillCacheMemoryPressure {

    public static final String CURRENT_BYTES_METRIC = "es.fill_cache.memory.current";
    public static final String WAITING_BYTES_METRIC = "es.fill_cache.memory.waiting.current";

    public static final Setting<ByteSizeValue> FILL_BYTES_LIMIT = Setting.memorySizeSetting(
        "stateless.fill_cache.memory.limit",
        "10%",
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(FillCacheMemoryPressure.class);

    private final long fillBytesLimit;
    private final Executor grantExecutor;
    private final LongUpDownCounter metricCurrentBytes;
    private final LongUpDownCounter metricWaitingBytes;

    private final Object mutex = new Object();
    // both guarded by mutex
    private long currentBytes = 0;
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    private record Waiter(long bytes, ActionListener<Releasable> listener) {}

    public FillCacheMemoryPressure(Settings settings, MeterRegistry meterRegistry, Executor grantExecutor) {
        this.fillBytesLimit = FILL_BYTES_LIMIT.get(settings).getBytes();
        this.grantExecutor = grantExecutor;
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
     * and no earlier acquirer is waiting; otherwise queued and completed on the grant executor, in FIFO order, as budget frees up.
     * A request larger than the whole limit is granted once nothing else is in flight, so it cannot wait forever.
     */
    public void acquire(long bytes, ActionListener<Releasable> listener) {
        assert bytes > 0 : "acquiring [" + bytes + "] bytes";
        synchronized (mutex) {
            // grant only if no one is already waiting, else a large head-of-queue waiter could starve
            if (waiters.isEmpty() && fits(bytes)) {
                grant(bytes);
            } else {
                waiters.addLast(new Waiter(bytes, listener));
                metricWaitingBytes.add(bytes);
                logger.trace(() -> Strings.format("queued fill read of [%d] bytes behind [%d] waiters", bytes, waiters.size() - 1));
                return;
            }
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
        // assert the budget is adjusted exactly once even if a caller both closes the stream and handles a failure
        return Releasables.assertOnce(() -> release(bytes));
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
                metricWaitingBytes.add(-head.bytes());
                grant(head.bytes());
                granted.add(head);
            }
        }
        // complete off-mutex and forked: a synchronously failing read would otherwise release (and drain) recursively
        for (Waiter waiter : granted) {
            grantExecutor.execute(() -> waiter.listener().onResponse(releasableFor(waiter.bytes())));
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
