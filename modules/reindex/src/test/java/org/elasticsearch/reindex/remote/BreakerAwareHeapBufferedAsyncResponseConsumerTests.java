/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.ContentTooLongException;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.reindex.remote.BreakerAwareHeapBufferedAsyncResponseConsumer.REMOTE_RESPONSE_BUFFER_BREAKER_LABEL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link BreakerAwareHeapBufferedAsyncResponseConsumer}.
 *
 * <p>Uses {@link TrackingBreaker} (the same helper used by {@link RemoteParseContextTests}) to
 * observe breaker interactions without spinning up a full cluster.
 */
public class BreakerAwareHeapBufferedAsyncResponseConsumerTests extends ESTestCase {

    /**
     * Minimal fake breaker: tracks net registered bytes and optionally trips when a limit is exceeded.
     * Identical in structure to the one in RemoteParseContextTests.
     */
    static class TrackingBreaker extends NoopCircuitBreaker {
        private final AtomicLong net = new AtomicLong();
        private final long limit;

        TrackingBreaker() {
            this(Long.MAX_VALUE);
        }

        TrackingBreaker(long limit) {
            super(CircuitBreaker.REQUEST);
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
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

    /** Convenience: create a 200-OK BasicHttpResponse with an entity whose Content-Length is {@code len}. */
    private static BasicHttpResponse responseWithContentLength(long len) {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "OK");
        BasicHttpResponse response = new BasicHttpResponse(statusLine);
        response.setEntity(new StringEntity("", ContentType.APPLICATION_JSON) {
            @Override
            public long getContentLength() {
                return len;
            }
        });
        return response;
    }

    /** Known Content-Length → the exact byte count is reserved. */
    public void testReservesContentLengthOnEnclosed() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 100 * 1024 * 1024;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        long contentLength = randomLongBetween(1, bufferLimit);
        consumer.responseReceived(responseWithContentLength(contentLength));

        assertThat("exact content-length reserved", breaker.getUsed(), equalTo(contentLength));
        assertThat(consumer.currentReservation(), equalTo(contentLength));
    }

    /**
     * Unknown Content-Length (chunked transfer encoding, Content-Length: -1) → no buffer-level
     * reservation. Per-hit {@code RemoteParseContext} accounting provides protection instead.
     */
    public void testNoReservationWhenContentLengthUnknown() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 1024;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        consumer.responseReceived(responseWithContentLength(-1));

        assertThat("no reservation for chunked response", breaker.getUsed(), equalTo(0L));
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    /** Zero Content-Length → treated the same as unknown length: no reservation. */
    public void testNoReservationWhenContentLengthZero() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 512;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        consumer.responseReceived(responseWithContentLength(0));

        assertThat("no reservation when content-length is 0", breaker.getUsed(), equalTo(0L));
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    /** When the breaker limit is too small, onEntityEnclosed propagates a CBE via IOException. */
    public void testTripsBreakerWhenReservationExceedsLimit() {
        int bufferLimit = 1024;
        TrackingBreaker breaker = new TrackingBreaker(512L); // less than bufferLimit
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        // Content-Length = 1024 > 512 limit → breaker trips
        IOException thrown = expectThrows(IOException.class, () -> consumer.responseReceived(responseWithContentLength(1024)));
        assertThat("IOException wraps CircuitBreakingException", thrown.getCause(), instanceOf(CircuitBreakingException.class));
        assertThat(thrown.getCause().getMessage(), containsString(REMOTE_RESPONSE_BUFFER_BREAKER_LABEL));
        assertThat("no bytes leaked to breaker on trip", breaker.getUsed(), equalTo(0L));
        assertThat("reservation field cleared", consumer.currentReservation(), equalTo(0L));
    }

    /** close() releases bytes reserved in onEntityEnclosed. */
    public void testCloseRefundsBreaker() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 100 * 1024;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        consumer.responseReceived(responseWithContentLength(bufferLimit));
        assertThat(breaker.getUsed(), equalTo((long) bufferLimit));

        consumer.close();
        assertThat("close() must refund the buffer reservation", breaker.getUsed(), equalTo(0L));
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    /** close() is idempotent via AbstractAsyncResponseConsumer — the second call is a no-op. */
    public void testCloseIsIdempotent() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 4096;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        consumer.responseReceived(responseWithContentLength(bufferLimit));
        consumer.close();
        consumer.close(); // second close must not double-refund
        assertThat("no double-refund on second close", breaker.getUsed(), equalTo(0L));
    }

    /** Content-Length > buffer cap triggers ContentTooLongException before any reservation. */
    public void testContentTooLongPropagatesWithoutReservation() {
        TrackingBreaker breaker = new TrackingBreaker();
        int bufferLimit = 1024;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, bufferLimit);

        // Content-Length exceeds the buffer cap — super throws ContentTooLongException, nothing reserved.
        expectThrows(ContentTooLongException.class, () -> consumer.responseReceived(responseWithContentLength(bufferLimit + 1)));
        assertThat("no bytes reserved when ContentTooLongException is thrown", breaker.getUsed(), equalTo(0L));
    }

    /** A NoopCircuitBreaker passes without tripping, and currentReservation tracks bytes correctly. */
    public void testNoopBreakerDoesNotTrip() throws Exception {
        CircuitBreaker noop = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        int bufferLimit = 100 * 1024;
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(noop, bufferLimit);

        consumer.responseReceived(responseWithContentLength(bufferLimit));
        // NoopCircuitBreaker always reports 0 used
        assertThat(noop.getUsed(), equalTo(0L));
        assertThat("reservation field still set", consumer.currentReservation(), equalTo((long) bufferLimit));
        consumer.close();
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    /** Constructor rejects non-positive bufferLimitBytes and a null breaker. */
    public void testConstructorValidation() {
        TrackingBreaker breaker = new TrackingBreaker();
        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 0));
        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, -1));
        expectThrows(NullPointerException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(null, 1024));
    }
}
