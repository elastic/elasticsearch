/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectByteBufferBodyHandlersTests extends ESTestCase {

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

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
            assertTrue(result.buffer().isDirect());
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
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
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

    public void testSkipThenFillAcrossChunks() throws Exception {
        byte[] fullBody = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "56789".getBytes(StandardCharsets.UTF_8);
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(5, expected.length, FACTORY);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody, 0, 7), ByteBuffer.wrap(fullBody, 7, fullBody.length - 7)));
        subscriber.onComplete();

        try (DirectReadBuffer result = subscriber.getBody().toCompletableFuture().get()) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(expected, toByteArray(result.buffer()));
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
            assertTrue(result.buffer().isDirect());
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
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(expected, toByteArray(result.buffer()));
        }
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
}
