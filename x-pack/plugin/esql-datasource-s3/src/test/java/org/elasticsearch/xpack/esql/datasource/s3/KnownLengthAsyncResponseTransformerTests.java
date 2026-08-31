/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link KnownLengthAsyncResponseTransformer}. The transformer is exercised
 * directly through its public {@link software.amazon.awssdk.core.async.AsyncResponseTransformer}
 * surface area (no real S3AsyncClient is needed); chunks are emitted via a hand-rolled
 * {@link SdkPublisher} that calls {@code onSubscribe}/{@code onNext}/{@code onComplete}
 * synchronously, mirroring the contract documented in the SDK.
 */
public class KnownLengthAsyncResponseTransformerTests extends ESTestCase {

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));

    public void testRejectsNegativeExpectedLength() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new KnownLengthAsyncResponseTransformer<>(-1, FACTORY)
        );
        assertThat(ex.getMessage(), containsString("must be non-negative"));
    }

    public void testSingleChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), List.of(ByteBuffer.wrap(payload)))) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testMultiChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), false);
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), chunks)) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testMultiChunkDirectByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), true);
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), chunks)) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testHeapByteBufferWithArrayOffset() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 1024));
        // Wrap a backing array with a leading slack so arrayOffset() is non-zero.
        int slack = between(1, 16);
        byte[] backing = new byte[payload.length + slack + between(0, 16)];
        System.arraycopy(payload, 0, backing, slack, payload.length);
        ByteBuffer chunk = ByteBuffer.wrap(backing, slack, payload.length).slice();
        assertTrue("test fixture should have hasArray=true", chunk.hasArray());
        assertThat(chunk.arrayOffset(), greaterThanOrEqualTo(slack));

        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), List.of(chunk))) {
            assertFalse(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testEmptyResponse() throws Exception {
        try (DirectReadBuffer result = runTransformer(0, response(0), List.of())) {
            assertFalse(result.buffer().isDirect());
            assertEquals(0, result.buffer().remaining());
        }
    }

    public void testOverflowFailsFastAndCancelsSubscription() {
        byte[] payload = randomByteArrayOfLength(64);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicLong requested = new AtomicLong(0);

        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            payload.length - 1,
            FACTORY
        );
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response(payload.length - 1));

        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        requested.addAndGet(n);
                    }

                    @Override
                    public void cancel() {
                        cancelled.set(true);
                    }
                });
                s.onNext(ByteBuffer.wrap(payload));
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("exceeded expected length"));
        assertTrue("subscription should be cancelled on overflow", cancelled.get());
        // The subscriber requests unbounded demand on subscribe (Reactive Streams §3.4); guard
        // against a future regression that adds backpressure without considering this contract.
        assertThat(requested.get(), equalTo(Long.MAX_VALUE));
    }

    public void testUnderflowOnCompleteFails() {
        byte[] partial = randomByteArrayOfLength(32);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            partial.length + 8,
            FACTORY
        );
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response(partial.length + 8));

        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onNext(ByteBuffer.wrap(partial));
                s.onComplete();
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("shorter than expected"));
    }

    public void testOnErrorPropagates() {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16, FACTORY);
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response(16));

        RuntimeException boom = new RuntimeException("boom");
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onError(boom);
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(boom, ex.getCause());
    }

    public void testExceptionOccurredBeforeStreamPropagates() {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16, FACTORY);
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();

        IllegalStateException error = new IllegalStateException("connection reset");
        transformer.exceptionOccurred(error);

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(error, ex.getCause());
    }

    /**
     * A transformer instance serves exactly one request attempt: a second {@code prepare()} is how
     * the SDK signals an internal retry with the same transformer, which would resurrect the
     * cross-attempt stale-{@code exceptionOccurred} race the single-use contract exists to prevent
     * (see the class javadoc). It must fail loudly rather than silently share state across attempts.
     */
    public void testPrepareIsSingleUse() {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(8, FACTORY);
        transformer.prepare();
        IllegalStateException ex = expectThrows(IllegalStateException.class, transformer::prepare);
        assertThat(ex.getMessage(), containsString("single-use"));
    }

    public void testOnCompleteReleasesBufferWhenItLosesTheCompletionRace() throws Exception {
        // If a concurrent exceptionOccurred fails the future before onComplete completes it, onComplete's
        // complete() returns false; it then solely owns the buffer it took via getAndSet and must release it,
        // or the breaker charge leaks.
        CircuitBreaker breaker = new LimitedBreaker("onComplete-race", ByteSizeValue.ofMb(16));
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);
        byte[] payload = randomByteArrayOfLength(256);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            payload.length,
            factory
        );
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response(payload.length));

        RuntimeException raced = new RuntimeException("exceptionOccurred won the completion race");
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onNext(ByteBuffer.wrap(payload)); // fills the destination: offset == capacity
                future.completeExceptionally(raced); // a concurrent exceptionOccurred fails the future first
                s.onComplete(); // onComplete loses the race; it must release the buffer it could not hand off
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(raced, ex.getCause());
        assertEquals("onComplete must release the buffer it could not hand off", 0L, breaker.getUsed());
    }

    /**
     * Verifies that {@code exceptionOccurred} is a no-op once the subscriber has already handled
     * its own terminal signal (the future is done). This is the {@code isDone()} guard that turns
     * netty's late duplicate notifications — the response-handler {@code onError} that follows the
     * subscriber's {@code onError} with the same throwable, and the channel-inactive teardown that
     * follows with a fresh {@code IOException} — into no-ops instead of double-frees or spurious
     * future completions.
     */
    public void testExceptionOccurredIsNoOpAfterSubscriberHandledError() throws Exception {
        CircuitBreaker breaker = new LimitedBreaker("test-breaker", ByteSizeValue.ofMb(16));
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16, factory);
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response(16));

        RuntimeException subscriberError = new RuntimeException("subscriber onError");
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onError(subscriberError);
            }
        });
        expectThrows(ExecutionException.class, future::get);
        assertEquals("subscriber should have released its buffer", 0L, breaker.getUsed());

        // Late duplicate #1: netty notifies the response handler with the throwable the subscriber
        // already handled. Late duplicate #2: channel-inactive teardown delivers a fresh IOException.
        // Both fire after the subscriber completed the future — each must be a no-op.
        transformer.exceptionOccurred(subscriberError);
        transformer.exceptionOccurred(new IOException("channel closed"));

        assertEquals("exceptionOccurred must not double-free", 0L, breaker.getUsed());
        // The future should still hold the original subscriber error, not a stale one.
        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(subscriberError, ex.getCause());
    }

    /**
     * Races {@code exceptionOccurred} (which calls {@code releaseOnFailure}) against a genuinely
     * concurrent {@code onNext} copy, with both threads released from the same barrier so the
     * subscriber lock is actually contended — no latch forces one side to finish first. The
     * destination buffer's release hook poisons the backing array while still inside the subscriber
     * lock, so the mutual-exclusion invariant becomes observable: after release has run, the array
     * must be entirely poison. If the copy could still write into the released buffer (the
     * use-after-free write this class guards against), payload bytes would overwrite the poison and
     * fail the assertion. Also verifies the buffer is released exactly once (no leak, no
     * double-free) and that the trailing subscriber {@code onError} after the race stays a no-op.
     */
    public void testExceptionOccurredContendingWithOnNextNeverWritesAfterRelease() throws Exception {
        final int size = 1024;
        final byte payloadByte = 0x5A;
        final byte poisonByte = (byte) 0xDE;
        byte[] payload = new byte[size];
        Arrays.fill(payload, payloadByte);

        int iterations = scaledRandomIntBetween(100, 500);
        for (int i = 0; i < iterations; i++) {
            byte[] backing = new byte[size];
            AtomicLong closeCount = new AtomicLong();
            DirectBufferFactory factory = len -> new DirectReadBuffer(ByteBuffer.wrap(backing), () -> {
                if (closeCount.incrementAndGet() == 1L) {
                    // Runs inside releaseOnFailure's subscriber lock: poisoning here models the
                    // allocator recycling the memory the instant it is freed.
                    Arrays.fill(backing, poisonByte);
                }
            });

            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(size, factory);
            CompletableFuture<DirectReadBuffer> future = transformer.prepare();
            transformer.onResponse(response(size));

            AtomicReference<Subscriber<? super ByteBuffer>> subscriberRef = new AtomicReference<>();
            transformer.onStream(new SdkPublisher<>() {
                @Override
                public void subscribe(Subscriber<? super ByteBuffer> s) {
                    s.onSubscribe(new TestSubscription());
                    subscriberRef.set(s);
                }
            });

            CyclicBarrier barrier = new CyclicBarrier(2);
            RuntimeException boom = new RuntimeException("concurrent exceptionOccurred");
            Thread copyThread = new Thread(() -> {
                await(barrier);
                subscriberRef.get().onNext(ByteBuffer.wrap(payload));
                // The stream error that would follow the abandoned publisher; must be a no-op
                // once exceptionOccurred already completed the future.
                subscriberRef.get().onError(new RuntimeException("stream error after race"));
            });
            Thread releaseThread = new Thread(() -> {
                await(barrier);
                transformer.exceptionOccurred(boom);
            });
            copyThread.start();
            releaseThread.start();
            copyThread.join();
            releaseThread.join();

            // exceptionOccurred always completes the future here (onNext alone never completes it).
            assertTrue(future.isCompletedExceptionally());
            assertEquals("buffer must be released exactly once", 1L, closeCount.get());
            // Mutual exclusion: the copy either fully preceded the release (poison overwrote it) or
            // was skipped after it. Any payload byte means a write into the released buffer.
            for (int b = 0; b < size; b++) {
                if (backing[b] != poisonByte) {
                    fail("write-after-free at offset " + b + " on iteration " + i + ": found " + backing[b]);
                }
            }
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testResponseObjectExposedViaGetter() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(8, 256));
        GetObjectResponse expectedResponse = response(payload.length);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            payload.length,
            FACTORY
        );
        try (DirectReadBuffer ignored = runTransformer(transformer, expectedResponse, List.of(ByteBuffer.wrap(payload)))) {
            assertThat(transformer.response().contentLength(), equalTo((long) payload.length));
        }
    }

    /**
     * Runs the transformer end-to-end given a fixed payload length, response, and list of
     * pre-computed chunks. Each chunk is emitted in order on the same calling thread, mirroring
     * the SDK's external-synchronization guarantee.
     */
    private static DirectReadBuffer runTransformer(int expectedLength, GetObjectResponse response, List<ByteBuffer> chunks)
        throws Exception {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            expectedLength,
            FACTORY
        );
        return runTransformer(transformer, response, chunks);
    }

    private static DirectReadBuffer runTransformer(
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer,
        GetObjectResponse response,
        List<ByteBuffer> chunks
    ) throws Exception {
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response);
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                for (ByteBuffer chunk : chunks) {
                    s.onNext(chunk);
                }
                s.onComplete();
            }
        });
        DirectReadBuffer result = future.get();
        assertFalse(result.buffer().isDirect());
        return result;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();
        return bytes;
    }

    private static GetObjectResponse response(int contentLength) {
        return GetObjectResponse.builder().contentLength((long) contentLength).build();
    }

    private static List<ByteBuffer> splitIntoChunks(byte[] payload, int chunkCount, boolean direct) {
        List<ByteBuffer> chunks = new ArrayList<>(chunkCount);
        int offset = 0;
        int remaining = payload.length;
        for (int i = 0; i < chunkCount && remaining > 0; i++) {
            int size = (i == chunkCount - 1) ? remaining : Math.max(1, remaining / (chunkCount - i));
            ByteBuffer chunk;
            if (direct) {
                chunk = ByteBuffer.allocateDirect(size);
                chunk.put(payload, offset, size);
                chunk.flip();
            } else {
                chunk = ByteBuffer.wrap(payload, offset, size).slice();
            }
            chunks.add(chunk);
            offset += size;
            remaining -= size;
        }
        return chunks;
    }

    /**
     * Inert subscription for tests that don't care about request/cancel signals.
     */
    private static final class TestSubscription implements Subscription {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
    }
}
