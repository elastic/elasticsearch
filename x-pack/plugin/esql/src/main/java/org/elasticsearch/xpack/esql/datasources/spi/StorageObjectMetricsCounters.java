/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for storage I/O. Provider implementations
 * hold one of these per {@link StorageObject} instance, increment it around each
 * I/O call, and surface the latest values via {@link #snapshot()} from
 * {@link StorageObject#metrics()}.
 * <p>
 * The split between this mutable struct and the immutable {@link StorageObjectMetrics}
 * snapshot mirrors the {@code Collector(LongAdder, LongAdder)} / {@code BlobStoreActionStats}
 * pattern used by ES repository plugins (see {@code GcsRepositoryStatsCollector}).
 * <p>
 * {@link LongAdder} is preferred over {@code AtomicLong} because async SDK callbacks
 * may concurrently increment from multiple threads and contention on a single AtomicLong
 * would dominate hot paths in object-store reads.
 * <p>
 * In addition to the profile snapshot, request/retry/bytes events are published to the node
 * {@link ExternalSourceMetrics} once a {@link Sink} is {@link #attach attached} (the operator wiring
 * does this when it opens a storage object). Until then the sink is {@link Sink#NONE} and the publishing
 * path is skipped entirely, so the profile-only behaviour is unchanged and allocation-free.
 */
public final class StorageObjectMetricsCounters {

    private final LongAdder requestCount = new LongAdder();
    private final LongAdder requestNanos = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder retryCount = new LongAdder();

    /**
     * Telemetry sink + its scheme dimension, held as one immutable value behind a single volatile so a
     * reader on an async callback thread never observes a new sink paired with a stale scheme.
     */
    private record Sink(ExternalSourceMetrics metrics, String scheme) {
        static final Sink NONE = new Sink(ExternalSourceMetrics.NOOP, "unknown");
    }

    // attach() runs on the operator thread that opens the object; addRequest()/addRetry()/addBytes() may
    // fire from async SDK or producer threads, so the sink is published through this single volatile field.
    private volatile Sink sink = Sink.NONE;

    /**
     * Planning holder captured on the coordinator thread that started the I/O, so native-async
     * completions (SDK / timer threads) still increment query planning totals after
     * {@link ExternalPlanningIo#activate} has been restored.
     */
    private volatile ExternalPlanningIo planningIo;

    /**
     * Set by {@link #attach}: this object is on the execution path. Planning totals must not
     * receive further events, even if a leaked or nested {@link ExternalPlanningIo#activate}
     * is visible on the current thread.
     */
    private volatile boolean executionAttached;

    /**
     * Attaches the node telemetry sink and the storage {@code scheme} dimension, so subsequent
     * request/retry/bytes events are published to {@link ExternalSourceMetrics} as well as the
     * profile snapshot. Idempotent; safe to call again as the same object is reused across reads.
     */
    public void attach(ExternalSourceMetrics metrics, String scheme) {
        this.sink = new Sink(metrics == null ? ExternalSourceMetrics.NOOP : metrics, scheme == null ? "unknown" : scheme);
        // Execution objects must not keep writing the coordinator planning holder.
        this.planningIo = null;
        this.executionAttached = true;
    }

    /** Pins the current planning holder so later async completions still count. */
    public void bindPlanningIo() {
        if (executionAttached) {
            return;
        }
        ExternalPlanningIo current = ExternalPlanningIo.current();
        if (current != null) {
            this.planningIo = current;
        }
    }

    /**
     * Sticky capture for planning I/O. After {@link #attach}, always {@code null} so a live
     * ThreadLocal on a shared worker cannot rebind execution bytes into planning totals.
     */
    private ExternalPlanningIo planningIo() {
        if (executionAttached) {
            return null;
        }
        ExternalPlanningIo live = ExternalPlanningIo.current();
        if (live != null) {
            this.planningIo = live;
            return live;
        }
        return planningIo;
    }

    /**
     * Records one completed request with its duration and, optionally, the bytes returned in the
     * same event.
     * <p>
     * Pass filled-buffer size when this event is the only byte source (native-async
     * {@code deliverRead}: one GET, one APM request+bytes event). Pass {@code bytes = 0} when
     * received body bytes are published separately via {@link #addBytes} and
     * {@link #publishStreamBytes}. Never call this with drained bytes at stream close — that
     * would double {@code storage.requests.total}.
     */
    public void addRequest(long durationNanos, long bytes) {
        requestCount.increment();
        if (durationNanos > 0) {
            requestNanos.add(durationNanos);
        }
        if (bytes > 0) {
            bytesRead.add(bytes);
        }
        ExternalPlanningIo io = planningIo();
        if (io != null) {
            io.recordRequest(bytes);
        }
        // Hot path: skip the publish (and its per-call work) entirely when no sink is attached — the unattached
        // case stays allocation-free. The record method self-guards, so no try/catch is needed here.
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordRequest(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, durationNanos)), bytes, s.scheme());
        }
    }

    /**
     * Adds received bytes to the profile snapshot only. Chunks published from a live stream must
     * not mint APM requests; {@link #publishStreamBytes} emits the APM bytes event once at close.
     */
    public void addBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        bytesRead.add(bytes);
        ExternalPlanningIo io = planningIo();
        if (io != null) {
            io.recordStreamBytes(bytes);
        }
    }

    /**
     * Publishes the stream's total received bytes to the node {@link ExternalSourceMetrics} sink
     * once (APM {@code storage.bytes_read.total} + usage). Does not increment the profile
     * {@link LongAdder} — callers already flushed those via {@link #addBytes}. No-op when no
     * sink is attached or {@code bytes <= 0}.
     */
    public void publishStreamBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordBytes(bytes, s.scheme());
        }
    }

    /** Records one automatic retry triggered inside an in-flight request. */
    public void addRetry() {
        retryCount.increment();
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordRetry(s.scheme());
        }
    }

    /**
     * Records one retry for the per-query <b>profile snapshot only</b> — bumps {@link #retryCount} but does
     * <b>not</b> publish to the node {@link ExternalSourceMetrics} sink. Used by metadata ops
     * ({@code length}/{@code lastModified}/{@code exists}) on {@code RetryableStorageObject}: those ops never bump the
     * read-scoped {@code requests.total}, so publishing their retries to the registry would leak
     * {@code storage.retries.total} past {@code storage.requests.total} (a scope violation on retryable providers). The
     * read path uses {@link #addRetry()} so its retries reach the registry as before.
     */
    public void addRetryProfileOnly() {
        retryCount.increment();
    }

    /**
     * Records one object-store read that exhausted retries and gave up terminally. Telemetry-only: it does
     * not touch the profile snapshot (only request/retry/bytes counters surface there). No-op when no sink
     * is attached; the record method self-guards so an instrumentation failure never breaks the read path.
     */
    public void addError() {
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordError(s.scheme());
        }
    }

    /** Records one object-store read whose terminal failure was a provider throttling response. Telemetry-only. */
    public void addThrottled() {
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordThrottled(s.scheme());
        }
    }

    /**
     * Records the cumulative time an object-store read spent in retry backoff. Telemetry-only; skipped when the
     * read never backed off ({@code millis <= 0}) so the histogram is not flooded with zero observations.
     */
    public void addReadStall(long millis) {
        if (millis <= 0) {
            return;
        }
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            s.metrics().recordReadStall(millis, s.scheme());
        }
    }

    /** Returns an immutable snapshot of the current counter values. */
    public StorageObjectMetrics snapshot() {
        return new StorageObjectMetrics(requestCount.sum(), requestNanos.sum(), bytesRead.sum(), retryCount.sum());
    }
}
