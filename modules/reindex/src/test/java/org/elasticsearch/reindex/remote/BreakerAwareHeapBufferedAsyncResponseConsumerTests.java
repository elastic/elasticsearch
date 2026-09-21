/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BufferedHttpEntity;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.reindex.remote.BreakerAwareHeapBufferedAsyncResponseConsumer.REMOTE_RESPONSE_BUFFER_BREAKER_LABEL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BreakerAwareHeapBufferedAsyncResponseConsumerTests extends ESTestCase {

    private static class TrackingBreaker extends NoopCircuitBreaker {
        private final AtomicLong used = new AtomicLong();
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
            long projected = used.get() + bytes;
            if (projected > limit) {
                throw new CircuitBreakingException(
                    "[" + label + "] tracking breaker tripped",
                    bytes,
                    limit,
                    CircuitBreaker.Durability.TRANSIENT
                );
            }
            used.addAndGet(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }

    private static class BlockingBreaker extends TrackingBreaker {
        private final CountDownLatch addEstimateStarted = new CountDownLatch(1);
        private final CountDownLatch unblockAddEstimate = new CountDownLatch(1);

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            // Hold the allocation path inside the breaker call so the test can
            // attempt failure cleanup while the allocator mutex is still held.
            addEstimateStarted.countDown();
            try {
                if (unblockAddEstimate.await(10, TimeUnit.SECONDS) == false) {
                    throw new AssertionError("timed out waiting to unblock addEstimateBytesAndMaybeBreak");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            super.addEstimateBytesAndMaybeBreak(bytes, label);
        }
    }

    // A decoder that simply advances the position in the buffer as bytes are read
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
        var breaker = new TrackingBreaker();
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

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

    public void testBufferingNonRepeatableEntityReleasesReservation() throws Exception {
        var breaker = new TrackingBreaker();
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

        consumer.responseReceived(responseWithContentLength(512));
        consumer.consumeContent(new FixedBytesContentDecoder(512), null);
        consumer.responseCompleted(null);

        HttpResponse result = consumer.getResult();
        assertThat(breaker.getUsed(), equalTo(512L));

        BufferedHttpEntity buffered = new BufferedHttpEntity(result.getEntity());
        assertThat(buffered.getContentLength(), equalTo(512L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testChunkedResponseGrowthIsAccountedAndReleasedWhenEntityIsClosed() throws Exception {
        var breaker = new TrackingBreaker();
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

        consumer.responseReceived(responseWithContentLength(-1));
        assertThat("unknown-length responses start with the REST client default initial buffer", breaker.getUsed(), equalTo(4096L));

        consumer.consumeContent(new FixedBytesContentDecoder(10_000), null);
        assertThat("buffer growth is accounted by current capacity", breaker.getUsed(), equalTo(16_390L));

        consumer.responseCompleted(null);
        ((Releasable) consumer.getResult().getEntity()).close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakerTripDuringChunkedGrowthIsReleasedOnFailure() throws Exception {
        var breaker = new TrackingBreaker(10_000L);
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

        consumer.responseReceived(responseWithContentLength(-1));
        assertThat(breaker.getUsed(), equalTo(4096L));

        CircuitBreakingException thrown = expectThrows(
            CircuitBreakingException.class,
            () -> consumer.consumeContent(new FixedBytesContentDecoder(10_000), null)
        );
        assertThat(thrown.getMessage(), containsString(REMOTE_RESPONSE_BUFFER_BREAKER_LABEL));

        consumer.failed(thrown);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testFailureBeforeResponseCompletionReleasesReservation() throws Exception {
        var breaker = new TrackingBreaker();
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

        consumer.responseReceived(responseWithContentLength(512));
        assertThat(breaker.getUsed(), equalTo(512L));

        consumer.failed(new IOException("boom"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testCloseWaitsForInProgressAllocationThenReleases() throws Exception {
        var breaker = new BlockingBreaker();
        var allocator = new BreakerAwareHeapBufferedAsyncResponseConsumer.AccountingByteBufferAllocator(breaker);

        // allocate() blocks inside the breaker call while holding the allocator mutex.
        Thread allocateThread = new Thread(() -> allocator.allocate(512), "allocate");
        Thread closeThread = new Thread(allocator::close, "close");
        try {
            allocateThread.start();
            assertTrue(breaker.addEstimateStarted.await(10, TimeUnit.SECONDS));

            // close() must wait for the in-progress allocation rather than draining the reservation
            // while allocate() is still mutating allocator state. This relies on the allocator holding
            // its mutex across addEstimateBytesAndMaybeBreak().
            closeThread.start();
            assertBusy(() -> assertThat(closeThread.getState(), equalTo(Thread.State.BLOCKED)));

            breaker.unblockAddEstimate.countDown();
            allocateThread.join(10_000);
            closeThread.join(10_000);

            assertFalse(allocateThread.isAlive());
            assertFalse(closeThread.isAlive());
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(allocator.currentReservation(), equalTo(0L));
        } finally {
            breaker.unblockAddEstimate.countDown();
            allocateThread.join(10_000);
            closeThread.join(10_000);
        }
    }

    public void testAllocatorReleasesAllOutstandingOnClose() {
        var breaker = new TrackingBreaker();
        var allocator = new BreakerAwareHeapBufferedAsyncResponseConsumer.AccountingByteBufferAllocator(breaker);

        allocator.allocate(100);
        allocator.allocate(50);
        assertThat(breaker.getUsed(), equalTo(150L));

        allocator.close();
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(allocator.currentReservation(), equalTo(0L));
    }

    public void testAllocateAfterCloseThrowsAndDoesNotChargeBreaker() {
        var breaker = new TrackingBreaker();
        var allocator = new BreakerAwareHeapBufferedAsyncResponseConsumer.AccountingByteBufferAllocator(breaker);

        allocator.allocate(100);
        allocator.close();
        assertThat(breaker.getUsed(), equalTo(0L));

        expectThrows(IllegalStateException.class, () -> allocator.allocate(4096));
        assertThat("allocate after close must not charge the breaker", breaker.getUsed(), equalTo(0L));
    }

    public void testReleaseAfterCloseDoesNotOverReleaseBreaker() {
        var breaker = new TrackingBreaker();
        var allocator = new BreakerAwareHeapBufferedAsyncResponseConsumer.AccountingByteBufferAllocator(breaker);

        allocator.allocate(100);
        allocator.close();
        assertThat(breaker.getUsed(), equalTo(0L));

        // A late release from an in-flight expand() that lost the race to close() must be a no-op.
        allocator.release(100);
        assertThat("release after close must not drive the breaker negative", breaker.getUsed(), equalTo(0L));
        assertThat(allocator.currentReservation(), equalTo(0L));
    }

    public void testAllocatorCloseIsIdempotent() {
        var breaker = new TrackingBreaker();
        var allocator = new BreakerAwareHeapBufferedAsyncResponseConsumer.AccountingByteBufferAllocator(breaker);

        allocator.allocate(100);
        allocator.close();
        allocator.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testEntityCloseIsIdempotent() throws Exception {
        var breaker = new TrackingBreaker();
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(breaker);

        consumer.responseReceived(responseWithContentLength(512));
        consumer.responseCompleted(null);
        Releasable releasable = (Releasable) consumer.getResult().getEntity();

        releasable.close();
        releasable.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testNoopBreakerDoesNotTrip() throws Exception {
        CircuitBreaker noop = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        var consumer = new BreakerAwareHeapBufferedAsyncResponseConsumer(noop);

        consumer.responseReceived(responseWithContentLength(1024));
        assertThat(noop.getUsed(), equalTo(0L));
        assertThat(consumer.currentReservation(), equalTo(1024L));

        consumer.responseCompleted(null);
        ((Releasable) consumer.getResult().getEntity()).close();
        assertThat(consumer.currentReservation(), equalTo(0L));
    }

    public void testConstructorValidation() {
        expectThrows(NullPointerException.class, () -> new BreakerAwareHeapBufferedAsyncResponseConsumer(null));
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
