/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3StorageObjectReadFailureTests extends ESTestCase {

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    public void testBareIllegalStateExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testSdkClientExceptionWrappingIllegalStateExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", ise);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertSame(wrapped, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertNotNull(ExceptionsHelper.unwrap(eue, IllegalStateException.class));
    }

    public void testNoSuchKeyStaysIoException() {
        S3Client mockS3 = mock(S3Client.class);
        NoSuchKeyException missing = NoSuchKeyException.builder().statusCode(404).message("Not Found").build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(missing);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException io = expectThrows(IOException.class, obj::newStream);
        assertEquals("Object not found: " + PATH, io.getMessage());
        assertSame(missing, io.getCause());
    }

    public void testProgrammingIllegalStateExceptionStays500() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("broken invariant");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IllegalStateException thrown = expectThrows(IllegalStateException.class, obj::newStream);
        assertSame(ise, thrown);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(thrown));
    }

    @SuppressWarnings("unchecked")
    public void testAsyncExternalUnavailableExceptionIsPreserved() throws Exception {
        S3Client mockS3 = mock(S3Client.class);
        S3AsyncClient mockAsyncS3 = mock(S3AsyncClient.class);
        ExternalUnavailableException expected = new ExternalUnavailableException(
            "S3 response body shorter than expected: received=5, expected=10"
        );
        CompletableFuture<DirectReadBuffer> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(expected);
        when(mockAsyncS3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(failedFuture);

        S3StorageObject obj = new S3StorageObject(mockS3, mockAsyncS3, BUCKET, KEY, PATH);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();

        obj.readBytesAsync(0, 10, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                buffer.close();
                outcome.set(new AssertionError("expected failure"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                outcome.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(expected, outcome.get());
    }

    public void testClosedClientOnLengthIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::length);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testClosedClientOnHeadFallbackIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception serverError = (S3Exception) S3Exception.builder().statusCode(500).message("Internal Error").build();
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(serverError);
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::length);
        assertSame(ise, eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
    }
}
