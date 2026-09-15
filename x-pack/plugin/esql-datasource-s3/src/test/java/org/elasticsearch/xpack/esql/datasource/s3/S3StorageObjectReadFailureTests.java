/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
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

    /**
     * The truncated-body case this class exists for, driven through the real
     * {@link KnownLengthAsyncResponseTransformer}: the store closes the body short of the requested range, so
     * {@code onComplete} arrives with fewer bytes than asked for. That has to reach the caller as the retryable
     * 503, both directly and after the operator's classification boundary — as a bare {@code IOException} it
     * was a client-class 400 and the retry layer never ran.
     */
    public void testAsyncShortBodyIsRetryable503() throws Exception {
        int requested = 10;
        Throwable thrown = readAsyncFailure(asyncClientEmitting(new byte[requested - 5], requested), requested);

        assertThat(thrown, instanceOf(ExternalUnavailableException.class));
        assertFalse(((ExternalUnavailableException) thrown).throttling());
        assertThat(thrown.getMessage(), containsString("shorter than expected"));
        // The sync path names the object in its transient-read failures; the async one must not be less useful
        // just because the exception is passed through failure mapping untouched.
        assertThat(thrown.getMessage(), containsString(PATH.toString()));
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(thrown));
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    /**
     * The 503 has to survive whatever shape the failure reaches the completion handler in. A single
     * {@code getCause()} peel there sees past the type in both of these — a typed exception carrying a cause of
     * its own, and one buried under an SDK wrapper — and the read is then given up on as a client-class 400.
     */
    public void testAsyncUnavailableSurvivesWrapping() throws Exception {
        ExternalUnavailableException withCause = new ExternalUnavailableException(
            "S3 response body shorter than expected reading [" + PATH + "]",
            new IOException("connection reset")
        );
        assertSame(withCause, readAsyncFailure(asyncClientFailingWith(withCause), 10));

        ExternalUnavailableException wrapped = new ExternalUnavailableException("S3 response body shorter than expected");
        Throwable sdkWrapped = new CompletionException(SdkClientException.create("Unable to execute HTTP request", wrapped));
        assertSame(wrapped, readAsyncFailure(asyncClientFailingWith(sdkWrapped), 10));
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

    /**
     * An async client that hands the transformer it is given a {@code body} of its own choosing, mirroring what the
     * SDK does on a successful HTTP exchange: prepare, unmarshalled response, then the body on the stream. Signals
     * are delivered synchronously on the calling thread, which the SDK's external-synchronization guarantee allows.
     */
    @SuppressWarnings("unchecked")
    private static S3AsyncClient asyncClientEmitting(byte[] body, int contentLength) {
        S3AsyncClient mockAsyncS3 = mock(S3AsyncClient.class);
        when(mockAsyncS3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            AsyncResponseTransformer<GetObjectResponse, DirectReadBuffer> transformer = invocation.getArgument(1);
            CompletableFuture<DirectReadBuffer> future = transformer.prepare();
            transformer.onResponse(GetObjectResponse.builder().contentLength((long) contentLength).build());
            transformer.onStream(new SdkPublisher<>() {
                @Override
                public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
                    subscriber.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {}

                        @Override
                        public void cancel() {}
                    });
                    subscriber.onNext(ByteBuffer.wrap(body));
                    subscriber.onComplete();
                }
            });
            return future;
        });
        return mockAsyncS3;
    }

    /** An async client whose read fails with {@code failure}, without the transformer ever being driven. */
    @SuppressWarnings("unchecked")
    private static S3AsyncClient asyncClientFailingWith(Throwable failure) {
        S3AsyncClient mockAsyncS3 = mock(S3AsyncClient.class);
        CompletableFuture<DirectReadBuffer> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        when(mockAsyncS3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(future);
        return mockAsyncS3;
    }

    /** Reads {@code length} bytes through the native async path and returns the failure handed to the listener. */
    private static Throwable readAsyncFailure(S3AsyncClient mockAsyncS3, int length) throws InterruptedException {
        S3StorageObject obj = new S3StorageObject(mock(S3Client.class), mockAsyncS3, BUCKET, KEY, PATH);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> outcome = new AtomicReference<>();

        obj.readBytesAsync(0, length, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                buffer.close();
                outcome.set(new AssertionError("expected the read to fail"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                outcome.set(e);
                latch.countDown();
            }
        });

        assertTrue("listener was never notified", latch.await(5, TimeUnit.SECONDS));
        return outcome.get();
    }
}
