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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.reindex.remote.RemoteParseContext.REMOTE_RESPONSE_BREAKER_LABEL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link RemoteParseContext} — the per-response circuit-breaker accounting
 * carrier used by reindex-from-remote response parsing.
 */
public class RemoteParseContextTests extends ESTestCase {

    /**
     * Test breaker that tracks the net bytes registered against it (positive on reserve,
     * negative on release) and optionally trips when the running total would exceed a limit.
     */
    private static class TrackingBreaker extends NoopCircuitBreaker {
        private final AtomicLong net = new AtomicLong();
        private final long limit;
        private final AtomicLong reserveCalls = new AtomicLong();

        TrackingBreaker() {
            this(Long.MAX_VALUE);
        }

        TrackingBreaker(long limit) {
            super(CircuitBreaker.REQUEST);
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            reserveCalls.incrementAndGet();
            long projected = net.get() + bytes;
            if (projected > limit) {
                throw new CircuitBreakingException(
                    "[" + label + "] tracking breaker tripped",
                    bytes,
                    limit,
                    CircuitBreaker.Durability.TRANSIENT
                );
            }
            net.addAndGet(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            net.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return net.get();
        }
    }

    /** Local accumulations smaller than the threshold do not consult the breaker. */
    public void testBelowThresholdDoesNotFlush() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 1024L)) {
            ctx.accountHit(100);
            ctx.accountHit(200);
            ctx.accountHit(300);
            assertThat("nothing should be flushed to the breaker yet", breaker.getUsed(), equalTo(0L));
            assertThat(breaker.reserveCalls.get(), equalTo(0L));
            assertThat(ctx.localAccumulatorBytes(), equalTo(600L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(0L));
        }
    }

    /** Crossing the threshold flushes the entire accumulator (not just the overflow) in one atomic step. */
    public void testCrossingThresholdFlushesAccumulator() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 1024L)) {
            ctx.accountHit(500);
            ctx.accountHit(600);
            assertThat("threshold crossed, accumulator drained to breaker", breaker.getUsed(), equalTo(1100L));
            assertThat("exactly one flush call", breaker.reserveCalls.get(), equalTo(1L));
            assertThat(ctx.localAccumulatorBytes(), equalTo(0L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(1100L));
        }
    }

    /** Multiple flushes accumulate against the breaker and totalRegistered tracks the sum. */
    public void testMultipleFlushesAccumulate() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 100L)) {
            ctx.accountHit(150);
            ctx.accountHit(150);
            ctx.accountHit(150);
            assertThat(breaker.getUsed(), equalTo(450L));
            assertThat(breaker.reserveCalls.get(), equalTo(3L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(450L));
        }
    }

    /** flushRemaining drains the tail-of-batch bytes that never crossed the threshold. */
    public void testFlushRemainingDrainsTail() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 1024L)) {
            ctx.accountHit(100);
            ctx.accountHit(200);
            ctx.flushRemaining();
            assertThat(breaker.getUsed(), equalTo(300L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(300L));
        }
    }

    /** flushRemaining with nothing in the accumulator is a no-op. */
    public void testFlushRemainingEmptyIsNoop() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 1024L)) {
            ctx.flushRemaining();
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(breaker.reserveCalls.get(), equalTo(0L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(0L));
        }
    }

    /** close() releases everything that was flushed to the breaker. */
    public void testCloseReleasesRegisteredBytes() {
        TrackingBreaker breaker = new TrackingBreaker();
        RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 100L);
        ctx.accountHit(500);
        assertThat(breaker.getUsed(), equalTo(500L));
        ctx.close();
        assertThat("close should release all registered bytes", breaker.getUsed(), equalTo(0L));
    }

    /** close() is idempotent — calling it twice does not double-release. */
    public void testCloseIsIdempotent() {
        TrackingBreaker breaker = new TrackingBreaker();
        RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 100L);
        ctx.accountHit(500);
        ctx.close();
        ctx.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * When the breaker trips during a flush, accountHit propagates the CircuitBreakingException
     * and totalRegistered reflects only the bytes successfully booked before the trip.
     */
    public void testCircuitBreakingExceptionPropagates() {
        TrackingBreaker breaker = new TrackingBreaker(1024L);
        RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 100L);
        try {
            ctx.accountHit(500);
            assertThat(breaker.getUsed(), equalTo(500L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(500L));
            CircuitBreakingException cbe = expectThrows(CircuitBreakingException.class, () -> ctx.accountHit(800));
            assertThat(cbe.getMessage(), containsString(REMOTE_RESPONSE_BREAKER_LABEL));
            // The failed flush registered nothing; totalRegistered is unchanged.
            assertThat(ctx.totalRegisteredBytes(), equalTo(500L));
            assertThat(breaker.getUsed(), equalTo(500L));
        } finally {
            // close() must still release the 500 that was successfully booked.
            ctx.close();
            assertThat("close after a CBE still releases successfully-booked bytes", breaker.getUsed(), equalTo(0L));
        }
    }

    /** A NoopCircuitBreaker neither trips nor reports usage; accounting becomes effectively free. */
    public void testNoopBreakerHasNoEffect() {
        CircuitBreaker noop = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, noop, 1L)) {
            for (int i = 0; i < 100; i++) {
                ctx.accountHit(10_000);
            }
            ctx.flushRemaining();
            assertThat(noop.getUsed(), equalTo(0L));
        }
    }

    /** Constructor rejects non-positive thresholds and null arguments. */
    public void testConstructorValidation() {
        TrackingBreaker breaker = new TrackingBreaker();
        expectThrows(IllegalArgumentException.class, () -> new RemoteParseContext(XContentType.JSON, breaker, 0L));
        expectThrows(IllegalArgumentException.class, () -> new RemoteParseContext(XContentType.JSON, breaker, -1L));
        expectThrows(NullPointerException.class, () -> new RemoteParseContext(null, breaker, 1L));
        expectThrows(NullPointerException.class, () -> new RemoteParseContext(XContentType.JSON, null, 1L));
    }

    /** Zero or negative byte accountings are ignored (defensive against bad callers). */
    public void testAccountHitIgnoresNonPositive() {
        TrackingBreaker breaker = new TrackingBreaker();
        try (RemoteParseContext ctx = new RemoteParseContext(XContentType.JSON, breaker, 1L)) {
            ctx.accountHit(0);
            ctx.accountHit(-100);
            assertThat(ctx.localAccumulatorBytes(), equalTo(0L));
            assertThat(ctx.totalRegisteredBytes(), equalTo(0L));
            assertThat(breaker.reserveCalls.get(), equalTo(0L));
        }
    }

    /** xContentType() exposes whatever the context was constructed with. */
    public void testXContentTypeAccessor() {
        TrackingBreaker breaker = new TrackingBreaker();
        XContentType type = randomFrom(XContentType.values());
        try (RemoteParseContext ctx = new RemoteParseContext(type, breaker, 1024L)) {
            assertThat(ctx.xContentType(), equalTo(type));
        }
    }
}
