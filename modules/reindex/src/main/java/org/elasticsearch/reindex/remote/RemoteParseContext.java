/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.XContentType;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parsing context for responses from a remote reindex cluster. Carries the {@link XContentType}
 * of the response and incrementally accounts the in-memory cost of parsed hits against the
 * {@link CircuitBreaker#REQUEST} circuit breaker as parsing proceeds.
 *
 * <p>Bytes are accumulated locally and only flushed to the breaker once the accumulator passes
 * a configurable threshold (see {@code cluster.reindex.memory_accounting_threshold}). This keeps
 * the cost of breaker bookkeeping bounded while still tripping early enough to prevent an
 * out-of-memory before the full hit batch is materialized — symmetric with what
 * {@code FetchPhase} does for local searches against the same breaker.
 *
 * <p>The context is a {@link Releasable}. On {@link #close()} it releases all bytes it has
 * registered with the breaker so far. Callers must guarantee that {@link #close()} is invoked
 * exactly once on every path:
 * <ul>
 *   <li>On a successful hit-bearing parse: handed off to
 *       {@link org.elasticsearch.reindex.PaginatedHitSource.Response#setBodyReleasable(Releasable)}
 *       so it lives as long as the batch.</li>
 *   <li>On a successful non-hit parse (e.g. version lookup, PIT open/close): closed immediately
 *       after the parse returns.</li>
 *   <li>On any parse failure: closed in the failure path.</li>
 * </ul>
 */
public final class RemoteParseContext implements Releasable {

    /** Label used when charging the REQUEST circuit breaker for remote reindex response bytes. */
    public static final String REMOTE_RESPONSE_BREAKER_LABEL = "reindex_remote_response";

    private final XContentType xContentType;
    private final CircuitBreaker breaker;
    private final long thresholdBytes;

    private final AtomicLong localAccumulator = new AtomicLong();
    private final AtomicLong totalRegistered = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param xContentType  content type of the response being parsed; never null
     * @param breaker       circuit breaker to charge; pass
     *                      {@link org.elasticsearch.common.breaker.NoopCircuitBreaker#NoopCircuitBreaker(String)}
     *                      to disable accounting in tests
     * @param thresholdBytes minimum local accumulation before flushing to the breaker; must be {@code > 0}
     */
    public RemoteParseContext(XContentType xContentType, CircuitBreaker breaker, long thresholdBytes) {
        this.xContentType = Objects.requireNonNull(xContentType, "xContentType");
        this.breaker = Objects.requireNonNull(breaker, "breaker");
        if (thresholdBytes <= 0) {
            throw new IllegalArgumentException("thresholdBytes must be > 0, was " + thresholdBytes);
        }
        this.thresholdBytes = thresholdBytes;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    /**
     * Record the estimated in-memory cost of a single parsed hit. If the local accumulator
     * crosses {@code thresholdBytes}, the accumulated bytes are flushed to the circuit breaker
     * via {@link CircuitBreaker#addEstimateBytesAndMaybeBreak(long, String)}.
     *
     * @throws CircuitBreakingException if the breaker trips during a flush. Bytes booked by
     *         previous successful flushes remain charged until {@link #close()} releases them.
     */
    public void accountHit(long bytes) throws CircuitBreakingException {
        if (bytes <= 0) {
            return;
        }
        if (localAccumulator.addAndGet(bytes) >= thresholdBytes) {
            // Drain atomically so concurrent callers (if any) don't double-flush the same bytes.
            long toFlush = localAccumulator.getAndSet(0);
            if (toFlush > 0) {
                breaker.addEstimateBytesAndMaybeBreak(toFlush, REMOTE_RESPONSE_BREAKER_LABEL);
                totalRegistered.addAndGet(toFlush);
            }
        }
    }

    /**
     * Flush whatever remains in the local accumulator to the breaker. Called by the parse driver
     * once parsing has produced a result, so the tail of bytes below the threshold also gets
     * accounted before the result is handed back to the caller.
     *
     * @throws CircuitBreakingException if the breaker trips while flushing the tail. Bytes booked
     *         by previous successful flushes remain charged until {@link #close()} releases them.
     */
    public void flushRemaining() throws CircuitBreakingException {
        long toFlush = localAccumulator.getAndSet(0);
        if (toFlush > 0) {
            breaker.addEstimateBytesAndMaybeBreak(toFlush, REMOTE_RESPONSE_BREAKER_LABEL);
            totalRegistered.addAndGet(toFlush);
        }
    }

    /**
     * Releases every byte registered with the breaker via {@link #accountHit(long)} or
     * {@link #flushRemaining()}. Idempotent: subsequent calls are no-ops.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            long total = totalRegistered.getAndSet(0);
            if (total > 0) {
                breaker.addWithoutBreaking(-total);
            }
        }
    }

    // Visible for testing.
    long localAccumulatorBytes() {
        return localAccumulator.get();
    }

    // Visible for testing.
    long totalRegisteredBytes() {
        return totalRegistered.get();
    }
}
