/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectByteBufferBodyHandlersTests extends ESTestCase {

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));

    /** Arbitrary non-zero slack, so a factory buffer that is larger than requested is not a rounding coincidence. */
    private static final int EXTRA_CAPACITY = 17;

    public void testFixedLengthSingleChunk() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            FACTORY
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testFixedLengthMultiChunk() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        int mid = payload.length / 2;
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            FACTORY
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload, 0, mid), ByteBuffer.wrap(payload, mid, payload.length - mid)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testFixedLengthOverAllocatedDestinationUsesExpectedLength() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(32, 512));
        AtomicInteger closeCalls = new AtomicInteger();
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            overAllocatingFactory(closeCalls)
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().get()) {
            assertEquals(payload.length + EXTRA_CAPACITY, result.buffer().capacity());
            assertEquals(payload.length, result.buffer().remaining());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
        assertEquals(1, closeCalls.get());
    }

    public void testFixedLengthRejectsUndersizedFactoryBufferAndCancels() {
        assertFixedLengthInvalidFactoryBufferRejected(ByteBuffer.allocate(15), 16);
    }

    public void testFixedLengthRejectsReadOnlyFactoryBufferAndCancels() {
        assertFixedLengthInvalidFactoryBufferRejected(ByteBuffer.allocateDirect(16).asReadOnlyBuffer(), 16);
    }

    public void testFixedLengthShortBodyFails() {
        // 206 path: server claimed Partial Content but delivered fewer bytes than expectedLength.
        // Must fail rather than silently return a short buffer, matching SkipThenFillDirectSubscriber
        // (200 fallback) and KnownLengthAsyncResponseTransformer (S3).
        byte[] payload = randomByteArrayOfLength(between(8, 64));
        int expectedLength = payload.length + between(1, 32);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            expectedLength,
            FACTORY
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("shorter than expected"));
        assertThat(ex.getCause().getMessage(), containsString("received=" + payload.length));
        assertThat(ex.getCause().getMessage(), containsString("expected=" + expectedLength));
    }

    public void testFixedLengthOverflowFails() {
        byte[] payload = randomByteArrayOfLength(32);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length - 1,
            FACTORY
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("exceeded expected length"));
    }

    public void testFixedLengthLateOnNextAfterCompleteIsIgnored() throws Exception {
        byte[] payload = randomByteArrayOfLength(32);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            FACTORY
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] { 1 })));

        try (DirectReadBuffer result = subscriber.getBody().get()) {
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testFixedLengthCancellationReleasesDestinationImmediately() {
        byte[] payload = randomByteArrayOfLength(32);
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = length -> new DirectReadBuffer(ByteBuffer.allocate(length), closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            factory
        );
        RecordingSubscription subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        assertTrue(subscriber.getBody().cancel(false));

        assertEquals(1, closeCalls.get());
        assertTrue(subscription.cancelled.get());
        subscriber.onComplete();
        assertEquals(1, closeCalls.get());
    }

    public void testFixedLengthCancelBeforeSubscribeDoesNotAllocate() {
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = length -> new DirectReadBuffer(ByteBuffer.allocate(length), closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            32,
            factory
        );
        assertTrue(subscriber.getBody().cancel(false));
        RecordingSubscription subscription = new RecordingSubscription();

        subscriber.onSubscribe(subscription);

        assertTrue(subscription.cancelled.get());
        assertEquals(0L, subscription.requested.get());
        assertEquals(0, closeCalls.get());
        assertTrue(subscriber.getBody().isCancelled());
    }

    public void testFixedLengthClosedResultIsNotRetainedBySubscriber() throws Exception {
        byte[] payload = randomByteArrayOfLength(1 << 20);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length,
            FACTORY
        );
        CompletableFuture<DirectReadBuffer> body = subscriber.getBody();
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        WeakReference<byte[]> destination = closeAndForgetDestination(body);
        try {
            assertBusy(() -> {
                System.gc();
                assertTrue("the completed body future must remain strongly reachable", body.isDone());
                assertNull("a closed fixed-length destination must be collectable", destination.get());
            });
        } finally {
            Reference.reachabilityFence(subscriber);
        }
    }

    public void testSkipThenFillAcrossChunks() throws Exception {
        byte[] fullBody = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "56789".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(5, expected.length, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody, 0, 7), ByteBuffer.wrap(fullBody, 7, fullBody.length - 7)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(expected, toByteArray(result.buffer()));
        }
    }

    public void testSkipThenFillOverAllocatedDestinationUsesExpectedLength() throws Exception {
        byte[] fullBody = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "56789".getBytes(StandardCharsets.UTF_8);
        AtomicInteger closeCalls = new AtomicInteger();
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(5, expected.length, overAllocatingFactory(closeCalls));
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().get()) {
            assertEquals(expected.length + EXTRA_CAPACITY, result.buffer().capacity());
            assertEquals(expected.length, result.buffer().remaining());
            assertArrayEquals(expected, toByteArray(result.buffer()));
        }
        assertEquals(1, closeCalls.get());
    }

    public void testSkipThenFillRejectsUndersizedFactoryBufferAndCancels() {
        assertSkipThenFillInvalidFactoryBufferRejected(ByteBuffer.allocate(15), 16);
    }

    public void testSkipThenFillRejectsReadOnlyFactoryBufferAndCancels() {
        assertSkipThenFillInvalidFactoryBufferRejected(ByteBuffer.allocateDirect(16).asReadOnlyBuffer(), 16);
    }

    public void testSkipThenFillLateOnNextAfterCompleteIsIgnored() throws Exception {
        byte[] fullBody = "0123456789".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "345".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(3, expected.length, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] { 1 })));

        try (DirectReadBuffer result = subscriber.getBody().get()) {
            assertArrayEquals(expected, toByteArray(result.buffer()));
        }
    }

    public void testSkipThenFillCancellationReleasesDestinationImmediately() {
        byte[] fullBody = "0123456789".getBytes(StandardCharsets.UTF_8);
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = length -> new DirectReadBuffer(ByteBuffer.allocate(length), closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(3, 3, factory);
        RecordingSubscription subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        assertTrue(subscriber.getBody().cancel(false));

        assertEquals(1, closeCalls.get());
        assertTrue(subscription.cancelled.get());
        subscriber.onComplete();
        assertEquals(1, closeCalls.get());
    }

    public void testSkipThenFillCancelBeforeSubscribeDoesNotAllocate() {
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = length -> new DirectReadBuffer(ByteBuffer.allocate(length), closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(3, 3, factory);
        assertTrue(subscriber.getBody().cancel(false));
        RecordingSubscription subscription = new RecordingSubscription();

        subscriber.onSubscribe(subscription);

        assertTrue(subscription.cancelled.get());
        assertEquals(0L, subscription.requested.get());
        assertEquals(0, closeCalls.get());
        assertTrue(subscriber.getBody().isCancelled());
    }

    public void testSkipThenFillClosedResultIsNotRetainedBySubscriber() throws Exception {
        int skip = 3;
        int length = 1 << 20;
        byte[] fullBody = randomByteArrayOfLength(skip + length);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(skip, length, FACTORY);
        CompletableFuture<DirectReadBuffer> body = subscriber.getBody();
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        WeakReference<byte[]> destination = closeAndForgetDestination(body);
        try {
            assertBusy(() -> {
                System.gc();
                assertTrue("the completed body future must remain strongly reachable", body.isDone());
                assertNull("a closed skip-then-fill destination must be collectable", destination.get());
            });
        } finally {
            Reference.reachabilityFence(subscriber);
        }
    }

    public void testSkipThenFillPositionBeyondBodyFails() {
        byte[] fullBody = "0123456789".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(20, 5, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("beyond content length"));
    }

    public void testSkipThenFillShortBodyAfterSkipFails() {
        // Skip 2 of 8, then ask for 8 more bytes — only 6 are available. Must fail rather than
        // silently return a short buffer, matching FixedLengthDirectSubscriber (206 path) and
        // KnownLengthAsyncResponseTransformer (S3). Downstream Parquet readers trust the
        // requested length when slicing the returned buffer.
        byte[] fullBody = "01234567".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(2, 8, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("shorter than expected"));
        assertThat(ex.getCause().getMessage(), containsString("received=6"));
        assertThat(ex.getCause().getMessage(), containsString("expected=8"));
    }

    public void testSkipThenFillAtEofWithNoBytesRemainingFails() {
        byte[] fullBody = "01234567".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(fullBody.length, 5, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        // Skip fully consumes the body, leaving zero bytes for the fill — fails via the strict
        // "shorter than expected" path (same as any other under-delivery after a successful skip).
        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("shorter than expected"));
        assertThat(ex.getCause().getMessage(), containsString("received=0"));
        assertThat(ex.getCause().getMessage(), containsString("expected=5"));
    }

    public void testRangeReadHandler206AccumulatesDirectBuffer() throws Exception {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(HttpStatus.SC_PARTIAL_CONTENT);
        HttpResponse.BodyHandler<DirectReadBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(0, payload.length, FACTORY);
        HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testRangeReadHandler404ReturnsDiscardingSubscriber() throws Exception {
        // Non-200 / non-206 status (e.g. 404, 500) must drain the body without allocating and
        // complete the body future with an empty buffer; the surrounding code in
        // HttpStorageObject.readBytesAsync then translates the status into listener.onFailure.
        int status = randomFrom(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_FORBIDDEN);
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(status);
        HttpResponse.BodyHandler<DirectReadBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(0, 1024, FACTORY);
        HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap("error page body".getBytes(StandardCharsets.UTF_8))));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertEquals(0, result.buffer().remaining());
        }
    }

    /**
     * Regression test: two consecutive failed HTTP requests (non-200/non-206 status) must each
     * produce an independent {@link DirectReadBuffer} so the caller can close them without
     * triggering the double-free tripwire.
     */
    public void testDiscardingSubscriberProducesFreshBufferPerResponse() throws Exception {
        int status = randomFrom(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_FORBIDDEN);
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(status);
        HttpResponse.BodyHandler<DirectReadBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(0, 1024, FACTORY);

        // Simulate two failed HTTP responses back-to-back (same JVM, same class statics).
        for (int i = 0; i < 2; i++) {
            HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
            subscriber.onSubscribe(new TestSubscription());
            subscriber.onComplete();
            DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get();
            assertEquals(0, result.buffer().remaining());
            // Must not throw AssertionError ("double-free") even on the second iteration.
            result.close();
        }
    }

    public void testRangeReadHandler200SkipsThenFills() throws Exception {
        byte[] fullBody = "0123456789".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "345".getBytes(StandardCharsets.UTF_8);
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(HttpStatus.SC_OK);
        HttpResponse.BodyHandler<DirectReadBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(3, expected.length, FACTORY);
        HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(expected, toByteArray(result.buffer()));
        }
    }

    private void assertFixedLengthInvalidFactoryBufferRejected(ByteBuffer invalidBuffer, int expectedLength) {
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = ignored -> new DirectReadBuffer(invalidBuffer, closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            expectedLength,
            factory
        );
        RecordingSubscription subscription = new RecordingSubscription();

        subscriber.onSubscribe(subscription);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("DirectBufferFactory"));
        assertEquals(1, closeCalls.get());
        assertTrue(subscription.cancelled.get());
        assertEquals(0L, subscription.requested.get());
    }

    private void assertSkipThenFillInvalidFactoryBufferRejected(ByteBuffer invalidBuffer, int length) {
        AtomicInteger closeCalls = new AtomicInteger();
        DirectBufferFactory factory = ignored -> new DirectReadBuffer(invalidBuffer, closeCalls::incrementAndGet);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(0, length, factory);
        RecordingSubscription subscription = new RecordingSubscription();

        subscriber.onSubscribe(subscription);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("DirectBufferFactory"));
        assertEquals(1, closeCalls.get());
        assertTrue(subscription.cancelled.get());
        assertEquals(0L, subscription.requested.get());
    }

    private static DirectBufferFactory overAllocatingFactory(AtomicInteger closeCalls) {
        return length -> {
            ByteBuffer destination = ByteBuffer.allocate(length + EXTRA_CAPACITY);
            destination.limit(1);
            return new DirectReadBuffer(destination, closeCalls::incrementAndGet);
        };
    }

    private static WeakReference<byte[]> closeAndForgetDestination(CompletableFuture<DirectReadBuffer> body) throws Exception {
        DirectReadBuffer result = body.get();
        WeakReference<byte[]> destination = new WeakReference<>(result.buffer().array());
        result.close();
        return destination;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static final class TestSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
    }

    private static final class RecordingSubscription implements Flow.Subscription {
        private final AtomicLong requested = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void request(long n) {
            requested.addAndGet(n);
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }
    }
}
