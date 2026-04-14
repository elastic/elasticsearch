/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for S3StorageObject async read paths and dual-client wiring.
 */
public class S3StorageObjectAsyncTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);
    private static final byte[] PAYLOAD = "Hello from S3 async".getBytes(StandardCharsets.UTF_8);

    private final S3Client mockSyncClient = mock(S3Client.class);
    private final S3AsyncClient mockAsyncClient = mock(S3AsyncClient.class);

    public void testSupportsNativeAsyncWithAsyncClient() {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);
        assertTrue(obj.supportsNativeAsync());
    }

    public void testSupportsNativeAsyncWithoutAsyncClient() {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, BUCKET, KEY, PATH);
        assertFalse(obj.supportsNativeAsync());
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncHappyPath() throws Exception {
        GetObjectResponse response = GetObjectResponse.builder()
            .contentRange("bytes 0-18/19")
            .contentLength((long) PAYLOAD.length)
            .lastModified(Instant.parse("2026-04-01T12:00:00Z"))
            .build();
        ResponseBytes<GetObjectResponse> responseBytes = ResponseBytes.fromByteArray(response, PAYLOAD);

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
            CompletableFuture.completedFuture(responseBytes)
        );

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ByteBuffer> result = new AtomicReference<>();

        obj.readBytesAsync(0, PAYLOAD.length, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(ByteBuffer buffer) {
                result.set(buffer);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        ByteBuffer buf = result.get();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        assertArrayEquals(PAYLOAD, bytes);
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncCachesMetadata() throws Exception {
        Instant lastModified = Instant.parse("2026-04-01T12:00:00Z");
        GetObjectResponse response = GetObjectResponse.builder().contentRange("bytes 0-18/1024").lastModified(lastModified).build();
        ResponseBytes<GetObjectResponse> responseBytes = ResponseBytes.fromByteArray(response, PAYLOAD);

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
            CompletableFuture.completedFuture(responseBytes)
        );

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        obj.readBytesAsync(0, PAYLOAD.length, Runnable::run, ActionListener.wrap(buf -> latch.countDown(), e -> fail()));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(1024L, obj.length());
        assertEquals(lastModified, obj.lastModified());
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncNotFound() throws Exception {
        CompletableFuture<ResponseBytes<GetObjectResponse>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(NoSuchKeyException.builder().statusCode(404).message("Not Found").build());

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(failedFuture);

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, 10, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(ByteBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(IOException.class));
        assertThat(error.get().getMessage(), containsString("Object not found"));
    }

    public void testReadBytesAsyncNegativePositionFails() throws Exception {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(-1, 10, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(ByteBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("position must be non-negative"));
    }

    public void testBothClientsClosedOnProviderClose() throws IOException {
        S3Client syncClient = mock(S3Client.class);
        S3AsyncClient asyncClient = mock(S3AsyncClient.class);

        S3StorageProvider provider = new S3StorageProvider(syncClient, asyncClient);
        provider.close();

        verify(syncClient).close();
        verify(asyncClient).close();
    }

    public void testAsyncClientClosedEvenIfSyncCloseThrows() {
        S3Client syncClient = mock(S3Client.class);
        S3AsyncClient asyncClient = mock(S3AsyncClient.class);

        doThrow(new RuntimeException("sync close failed")).when(syncClient).close();

        S3StorageProvider provider = new S3StorageProvider(syncClient, asyncClient);
        expectThrows(RuntimeException.class, provider::close);

        verify(asyncClient).close();
    }
}
