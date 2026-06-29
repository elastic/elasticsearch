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
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.ContentDecoder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.reindex.remote.BreakerAwareHeapBufferedAsyncResponseConsumer.REMOTE_RESPONSE_BUFFER_BREAKER_LABEL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BreakerAwareHeapBufferedAsyncResponseConsumerTests extends ESTestCase {

    private static class TrackingBreaker extends NoopCircuitBreaker {
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

    private static class FixedBytesContentDecoder implements ContentDecoder {
        private int remainingBytes;
        private boolean completed;

        FixedBytesContentDecoder(int bytes) {
            this.remainingBytes = bytes;
        }

        @Override
        public int read(ByteBuffer dst) {
            if (remainingBytes == 0) {
                completed = true;
                return -1;
            }
            if (dst.hasRemaining() == false) {
                return 0;
            }
            int bytes = Math.min(dst.remaining(), remainingBytes);
            dst.position(dst.position() + bytes);
            remainingBytes -= bytes;
            return bytes;
        }

        @Override
        public boolean isCompleted() {
            return completed || remainingBytes == 0;
        }
    }

    public void testKnownContentLengthReservationIsReleasedWhenEntityIsClosed() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 1024);

        consumer.responseReceived(responseWithContentLength(512));
        assertThat(breaker.getUsed(), equalTo(512L));
        assertThat(consumer.currentReservation(), equalTo(512L));

        consumer.responseCompleted(null);
        assertThat("successful response remains buffered for the caller to parse", breaker.getUsed(), equalTo(512L));

        HttpResponse result = consumer.getResult();
        assertThat(result.getEntity(), instanceOf(Releasable.class));
        ((Releasable) result.getEntity()).close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testChunkedResponseGrowthIsAccountedAndReleasedWhenEntityIsClosed() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 20_000);

        consumer.responseReceived(responseWithContentLength(-1));
        assertThat("unknown-length responses start with the REST client default initial buffer", breaker.getUsed(), equalTo(4096L));

        consumer.consumeContent(new FixedBytesContentDecoder(10_000), null);
        assertThat("buffer growth is accounted by current capacity", breaker.getUsed(), equalTo(16_390L));

        consumer.responseCompleted(null);
        ((Releasable) consumer.getResult().getEntity()).close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakerTripDuringChunkedGrowthIsReleasedOnFailure() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker(10_000L);
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 20_000);

        consumer.responseReceived(responseWithContentLength(-1));
        assertThat(breaker.getUsed(), equalTo(4096L));

        IOException thrown = expectThrows(IOException.class, () -> consumer.consumeContent(new FixedBytesContentDecoder(10_000), null));
        assertThat(thrown.getCause(), instanceOf(CircuitBreakingException.class));
        assertThat(thrown.getCause().getMessage(), containsString(REMOTE_RESPONSE_BUFFER_BREAKER_LABEL));

        consumer.failed(thrown);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testChunkedResponseIsCappedAtBufferLimit() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 8194);

        consumer.responseReceived(responseWithContentLength(-1));
        ContentTooLongException thrown = expectThrows(
            ContentTooLongException.class,
            () -> consumer.consumeContent(new FixedBytesContentDecoder(10_000), null)
        );
        assertThat(thrown.getMessage(), containsString("response buffer exceeded limit [8194] bytes"));

        consumer.failed(thrown);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testKnownContentLengthTooLongDoesNotReserveBytes() {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 1024);

        expectThrows(ContentTooLongException.class, () -> consumer.responseReceived(responseWithContentLength(1025)));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testFailureBeforeResponseCompletionReleasesReservation() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 1024);

        consumer.responseReceived(responseWithContentLength(512));
        assertThat(breaker.getUsed(), equalTo(512L));

        consumer.failed(new IOException("boom"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testEntityCloseIsIdempotent() throws Exception {
        TrackingBreaker breaker = new TrackingBreaker();
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 1024);

        consumer.responseReceived(responseWithContentLength(512));
        consumer.responseCompleted(null);
        Releasable releasable = (Releasable) consumer.getResult().getEntity();

        releasable.close();
        releasable.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testNoopBreakerDoesNotTrip() throws Exception {
        CircuitBreaker noop = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(noop, 1024);

        consumer.responseReceived(responseWithContentLength(1024));
        assertThat(noop.getUsed(), equalTo(0L));
        assertThat(consumer.currentReservation(), equalTo(1024L));

        consumer.responseCompleted(null);
        ((Releasable) consumer.getResult().getEntity()).close();
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    public void testConstructorValidation() {
        TrackingBreaker breaker = new TrackingBreaker();

        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, 0));
        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker, -1));
        expectThrows(NullPointerException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(null, 1024));
    }

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
}
