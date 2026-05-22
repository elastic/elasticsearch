/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectByteBufferBodyHandlersTests extends ESTestCase {

    public void testFixedLengthSingleChunk() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        ByteBuffer result = subscriber.getBody().toCompletableFuture().get();
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testFixedLengthMultiChunk() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        int mid = payload.length / 2;
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload, 0, mid), ByteBuffer.wrap(payload, mid, payload.length - mid)));
        subscriber.onComplete();

        ByteBuffer result = subscriber.getBody().toCompletableFuture().get();
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testFixedLengthOverflowFails() {
        byte[] payload = randomByteArrayOfLength(32);
        DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber subscriber = new DirectByteBufferBodyHandlers.FixedLengthDirectSubscriber(
            payload.length - 1
        );
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));

        ExecutionException ex = expectThrows(ExecutionException.class, () -> subscriber.getBody().get());
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("exceeded expected length"));
    }

    public void testSkipThenFillAcrossChunks() throws Exception {
        byte[] fullBody = "0123456789ABCDEFGHIJ".getBytes();
        byte[] expected = "56789".getBytes();
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(5, expected.length);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody, 0, 7), ByteBuffer.wrap(fullBody, 7, fullBody.length - 7)));
        subscriber.onComplete();

        ByteBuffer result = subscriber.getBody().toCompletableFuture().get();
        assertTrue(result.isDirect());
        assertArrayEquals(expected, toByteArray(result));
    }

    public void testSkipThenFillPositionBeyondBodyFails() {
        byte[] fullBody = "0123456789".getBytes();
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(20, 5);
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
        byte[] fullBody = "01234567".getBytes();
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(2, 8);
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
        byte[] fullBody = "01234567".getBytes();
        DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber subscriber =
            new DirectByteBufferBodyHandlers.SkipThenFillDirectSubscriber(fullBody.length, 5);
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
        byte[] payload = "hello".getBytes();
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(HttpStatus.SC_PARTIAL_CONTENT);
        HttpResponse.BodyHandler<ByteBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(0, payload.length);
        HttpResponse.BodySubscriber<ByteBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(payload)));
        subscriber.onComplete();

        ByteBuffer result = subscriber.getBody().toCompletableFuture().get();
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testRangeReadHandler200SkipsThenFills() throws Exception {
        byte[] fullBody = "0123456789".getBytes();
        byte[] expected = "345".getBytes();
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(HttpStatus.SC_OK);
        HttpResponse.BodyHandler<ByteBuffer> handler = DirectByteBufferBodyHandlers.ofRangeRead(3, expected.length);
        HttpResponse.BodySubscriber<ByteBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new TestSubscription());
        subscriber.onNext(List.of(ByteBuffer.wrap(fullBody)));
        subscriber.onComplete();

        ByteBuffer result = subscriber.getBody().toCompletableFuture().get();
        assertTrue(result.isDirect());
        assertArrayEquals(expected, toByteArray(result));
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
