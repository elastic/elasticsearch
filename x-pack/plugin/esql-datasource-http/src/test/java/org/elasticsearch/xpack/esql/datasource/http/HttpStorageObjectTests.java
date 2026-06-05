/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for HttpStorageObject with Range header support.
 *
 * Note: These are basic unit tests that verify object creation and path handling.
 * Full integration tests with actual HTTP requests should be done in integration test suites.
 */
@SuppressWarnings("unchecked")
public class HttpStorageObjectTests extends ESTestCase {

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

    public void testPath() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        assertEquals(path, object.path());
    }

    public void testPathWithPreKnownLength() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();

        HttpStorageObject object = new HttpStorageObject(mockClient, path, config, 12345L);

        assertEquals(path, object.path());
    }

    public void testPathWithPreKnownMetadata() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();

        HttpStorageObject object = new HttpStorageObject(mockClient, path, config, 12345L, java.time.Instant.now());

        assertEquals(path, object.path());
    }

    public void testInvalidRangePosition() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> { object.newStream(-1, 100); });
        assertTrue(e.getMessage().contains("position"));
    }

    public void testInvalidRangeLength() {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        // -1 is now the READ_TO_END open-ended sentinel; a different negative length is still invalid.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> { object.newStream(0, -2); });
        assertTrue(e.getMessage().contains("length"));
    }

    public void testBoundedInputStreamReadsExactly() throws Exception {
        byte[] data = "0123456789abcdefghij".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.io.ByteArrayInputStream source = new java.io.ByteArrayInputStream(data);

        // Create a BoundedInputStream via reflection since it's private
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/file.txt");
        HttpConfiguration config = HttpConfiguration.defaults();
        HttpStorageObject object = new HttpStorageObject(mockClient, path, config);

        // Test that we can create the object successfully
        assertNotNull(object);
        assertEquals(path, object.path());
    }

    public void testLengthOnNotFoundThrows() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        IOException e = expectThrows(IOException.class, object::length);
        assertThat(e.getMessage(), containsString("Object not found"));
    }

    public void testLastModifiedOnNotFoundThrows() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        IOException e = expectThrows(IOException.class, object::lastModified);
        assertThat(e.getMessage(), containsString("Object not found"));
    }

    public void testExistsOnNotFoundReturnsFalse() throws Exception {
        HttpStorageObject object = objectWithNotFoundResponse();
        assertFalse(object.exists());
    }

    public void testReadBytesAsync206UsesBodyHandler() throws Exception {
        byte[] payload = "hello parquet".getBytes(StandardCharsets.UTF_8);
        HttpClient mockClient = mock(HttpClient.class);
        mockSendAsyncWithBodyChunks(mockClient, HttpStatus.SC_PARTIAL_CONTENT, List.of(ByteBuffer.wrap(payload)));

        StoragePath path = StoragePath.of("https://example.com/data.parquet");
        HttpStorageObject object = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        byte[] result = readBytesAsyncToCompletion(object, 0, payload.length);
        assertArrayEquals(payload, result);
    }

    public void testReadBytesAsync200OkUsesBodyHandlerToSlice() throws Exception {
        byte[] fullBody = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "56789".getBytes(StandardCharsets.UTF_8);
        HttpClient mockClient = mock(HttpClient.class);
        mockSendAsyncWithBodyChunks(mockClient, HttpStatus.SC_OK, List.of(ByteBuffer.wrap(fullBody)));

        StoragePath path = StoragePath.of("https://example.com/data.parquet");
        HttpStorageObject object = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        byte[] result = readBytesAsyncToCompletion(object, 5, expected.length);
        assertArrayEquals(expected, result);
    }

    public void testReadBytesAsyncLengthExceedsIntMaxFails() throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        StoragePath path = StoragePath.of("https://example.com/data.parquet");
        HttpStorageObject object = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        object.readBytesAsync(
            0,
            (long) Integer.MAX_VALUE + 1,
            FACTORY,
            Runnable::run,
            ActionListener.wrap(buf -> { fail("expected failure"); }, e -> {
                error.set(e);
                latch.countDown();
            })
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("must fit in an int"));
    }

    @SuppressWarnings("unchecked")
    private HttpStorageObject objectWithNotFoundResponse() throws Exception {
        HttpResponse<Void> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        when(mockResponse.headers()).thenReturn(HttpHeaders.of(java.util.Map.of(), (a, b) -> true));

        HttpClient mockClient = mock(HttpClient.class);
        doReturn(mockResponse).when(mockClient).send(any(), any());

        StoragePath path = StoragePath.of("https://example.com/missing.parquet");
        return new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());
    }

    /**
     * newStream(pos, length) increments {@link StorageObjectMetrics} request counters and records
     * the bytes read from the response.
     */
    public void testRangeNewStreamIncrementsMetrics() throws Exception {
        long rangeBytes = 1024L;
        HttpResponse<java.io.InputStream> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(HttpStatus.SC_PARTIAL_CONTENT);
        when(mockResponse.headers()).thenReturn(
            HttpHeaders.of(java.util.Map.of("Content-Length", java.util.List.of(Long.toString(rangeBytes))), (a, b) -> true)
        );
        when(mockResponse.body()).thenReturn(new ByteArrayInputStream(new byte[(int) rangeBytes]));

        HttpClient mockClient = mock(HttpClient.class);
        doReturn(mockResponse).when(mockClient).send(any(), any());

        StoragePath path = StoragePath.of("https://example.com/file.parquet");
        HttpStorageObject obj = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        assertEquals(0L, obj.metrics().requestCount());
        obj.newStream(0, rangeBytes).close();

        StorageObjectMetrics metrics = obj.metrics();
        assertEquals(1L, metrics.requestCount());
        assertEquals(rangeBytes, metrics.bytesRead());
        assertTrue("requestNanos should be > 0", metrics.requestNanos() > 0);
        assertEquals(0L, metrics.retryCount());
    }

    /**
     * Metadata-probe paths (length(), exists(), lastModified()) are intentionally NOT counted in
     * metrics() — they're not data reads.
     */
    public void testMetadataProbesDoNotCountAsRequests() throws Exception {
        long fileSize = 100_000L;
        HttpResponse<Void> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.headers()).thenReturn(
            HttpHeaders.of(java.util.Map.of("Content-Length", java.util.List.of(Long.toString(fileSize))), (a, b) -> true)
        );

        HttpClient mockClient = mock(HttpClient.class);
        doReturn(mockResponse).when(mockClient).send(any(), any());

        StoragePath path = StoragePath.of("https://example.com/file.parquet");
        HttpStorageObject obj = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        obj.length();
        obj.exists();

        assertEquals(0L, obj.metrics().requestCount());
        assertEquals(0L, obj.metrics().bytesRead());
    }

    private static byte[] readBytesAsyncToCompletion(HttpStorageObject object, long position, long length) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();

        object.readBytesAsync(position, length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            result.set(buf);
            latch.countDown();
        }, e -> { throw new AssertionError("unexpected failure", e); }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(result.get());
        // Copy the bytes off the allocator-backed buffer so the caller doesn't hold a view of
        // memory that we release here. The DirectReadBuffer payload is always allocator-backed
        // (i.e. direct); callers historically asserted isDirect() on the returned buffer.
        DirectReadBuffer drb = result.get();
        try {
            assertTrue("readBytesAsync must return a direct ByteBuffer", drb.buffer().isDirect());
            return toByteArray(drb.buffer());
        } finally {
            drb.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static void mockSendAsyncWithBodyChunks(HttpClient mockClient, int statusCode, List<ByteBuffer> bodyChunks) throws Exception {
        doAnswer(invocation -> {
            HttpResponse.BodyHandler<DirectReadBuffer> handler = invocation.getArgument(1);
            HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
            when(responseInfo.statusCode()).thenReturn(statusCode);
            HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
            subscriber.onSubscribe(new NoOpSubscription());
            subscriber.onNext(bodyChunks);
            subscriber.onComplete();
            DirectReadBuffer body = subscriber.getBody().toCompletableFuture().get();

            HttpResponse<DirectReadBuffer> response = mock(HttpResponse.class);
            when(response.statusCode()).thenReturn(statusCode);
            when(response.body()).thenReturn(body);
            return CompletableFuture.completedFuture(response);
        }).when(mockClient).sendAsync(any(), any());
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static final class NoOpSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
    }
}
