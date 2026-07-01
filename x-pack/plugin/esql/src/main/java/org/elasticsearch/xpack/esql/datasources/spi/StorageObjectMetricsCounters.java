/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

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
 * In addition to the profile snapshot, the same request/retry events are published to the node
 * {@link ExternalSourceMetrics} once a {@link Sink} is {@link #attach attached} (the operator wiring
 * does this when it opens a storage object). Until then the sink is {@link Sink#NONE} and the publishing
 * path is skipped entirely, so the profile-only behaviour is unchanged and allocation-free.
 */
public final class StorageObjectMetricsCounters {

    private static final Logger logger = LogManager.getLogger(StorageObjectMetricsCounters.class);

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

    // attach() runs on the operator thread that opens the object; addRequest()/addRetry() may fire from
    // async SDK callback threads, so the sink is published through this single volatile field.
    private volatile Sink sink = Sink.NONE;

    /**
     * Attaches the node telemetry sink and the storage {@code scheme} dimension, so subsequent
     * request/retry events are published to {@link ExternalSourceMetrics} as well as the profile
     * snapshot. Idempotent; safe to call again as the same object is reused across reads.
     */
    public void attach(ExternalSourceMetrics metrics, String scheme) {
        this.sink = new Sink(metrics == null ? ExternalSourceMetrics.NOOP : metrics, scheme == null ? "unknown" : scheme);
    }

    /** Records one completed request with its duration and the bytes returned. */
    public void addRequest(long durationNanos, long bytes) {
        requestCount.increment();
        if (durationNanos > 0) {
            requestNanos.add(durationNanos);
        }
        if (bytes > 0) {
            bytesRead.add(bytes);
        }
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            // Best-effort: an instrumentation failure must never break the read path.
            try {
                s.metrics().recordRequest(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, durationNanos)), bytes, s.scheme());
            } catch (Exception e) {
                logger.trace("telemetry: recordRequest failed", e);
            }
        }
    }

    /** Records one automatic retry triggered inside an in-flight request. */
    public void addRetry() {
        retryCount.increment();
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            try {
                s.metrics().recordRetry(s.scheme());
            } catch (Exception e) {
                logger.trace("telemetry: recordRetry failed", e);
            }
        }
    }

    /**
     * Records one object-store read that exhausted retries and gave up terminally. Telemetry-only: it does
     * not touch the profile snapshot (only request/retry/bytes counters surface there). No-op when no sink
     * is attached; best-effort so an instrumentation failure never breaks the read path.
     */
    public void addError() {
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            try {
                s.metrics().recordError(s.scheme());
            } catch (Exception e) {
                logger.trace("telemetry: recordError failed", e);
            }
        }
    }

    /** Records one object-store read whose terminal failure was a provider throttling response. Telemetry-only. */
    public void addThrottled() {
        Sink s = sink;
        if (s.metrics() != ExternalSourceMetrics.NOOP) {
            try {
                s.metrics().recordThrottled(s.scheme());
            } catch (Exception e) {
                logger.trace("telemetry: recordThrottled failed", e);
            }
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
            try {
                s.metrics().recordReadStall(millis, s.scheme());
            } catch (Exception e) {
                logger.trace("telemetry: recordReadStall failed", e);
            }
        }
    }

    /** Returns an immutable snapshot of the current counter values. */
    public StorageObjectMetrics snapshot() {
        return new StorageObjectMetrics(requestCount.sum(), requestNanos.sum(), bytesRead.sum(), retryCount.sum());
    }
}
