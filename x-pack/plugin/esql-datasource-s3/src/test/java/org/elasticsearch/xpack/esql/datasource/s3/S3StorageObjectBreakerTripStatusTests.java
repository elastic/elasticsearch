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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduction: a circuit-breaker rejection of the "storage read buffer" charge on the S3
 * native-async read path must surface with breaker status (429), not be buried inside an
 * {@link java.io.IOException} that the read boundary classifies as a client error (400).
 *
 * <p>The destination buffer for a native-async S3 read is allocated inside the AWS SDK's
 * response pipeline ({@code KnownLengthAsyncResponseTransformer}'s subscriber, on
 * {@code onSubscribe}). When the breaker refuses that charge, the SDK's retry stage
 * ({@code RetryableStageHelper}, sdk-core 2.31.78) wraps the {@link CircuitBreakingException}
 * in {@code SdkClientException("Unable to execute HTTP request: ...")}. This test mimics that
 * exact wrapping (verified against the sdk-core bytecode) and asserts the mapped failure
 * delivered to the listener still carries 429.
 */
public class S3StorageObjectBreakerTripStatusTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/hits.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    @SuppressWarnings("unchecked")
    public void testBreakerTripInsideSdkPipelineSurfacesWithBreakerStatus() throws Exception {
        CircuitBreaker refusing = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                throw new CircuitBreakingException(
                    "[parent] Data too large, data for [" + label + "] would be [4093440261/3.8gb]",
                    bytes,
                    0,
                    CircuitBreaker.Durability.TRANSIENT
                );
            }
        };
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(refusing);

        S3Client mockSyncClient = mock(S3Client.class);
        S3AsyncClient mockAsyncClient = mock(S3AsyncClient.class);
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(
            invocation -> driveLikeTheSdk(invocation.getArgument(1))
        );

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        obj.readBytesAsync(0, 1024, factory, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // The rejection must still be in the cause chain, whatever the wrapping did.
        assertNotNull(
            "the CircuitBreakingException must survive in the cause chain",
            ExceptionsHelper.unwrap(error.get(), CircuitBreakingException.class)
        );

        // The mapped failure must carry the breaker's status, not be buried under a
        // status-neutral IOException (status 500 here, and classified 400 by the external
        // read boundary).
        assertThat(
            "a breaker rejection must surface with breaker status, not as an I/O failure",
            ExceptionsHelper.status(error.get()),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );
        // ... and still name the object that tripped it, like every other mapped read failure.
        assertThat(error.get(), instanceOf(CircuitBreakingException.class));
        assertThat(error.get().getMessage(), containsString(PATH.toString()));
    }

    /**
     * Drives the transformer the way the AWS SDK does, including the failure wrapping applied by
     * {@code software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper}:
     * a non-SdkException attempt failure becomes
     * {@code SdkClientException.create("Unable to execute HTTP request: " + msg, cause)}.
     */
    private static CompletableFuture<DirectReadBuffer> driveLikeTheSdk(KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer) {
        CompletableFuture<DirectReadBuffer> prepared = transformer.prepare();
        transformer.onResponse(GetObjectResponse.builder().contentRange("bytes 0-1023/9663676416").build());
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                // onSubscribe allocates the destination buffer; the refusing breaker throws there
                // and the subscriber routes the failure into the prepared future.
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {}

                    @Override
                    public void cancel() {}
                });
            }
        });
        CompletableFuture<DirectReadBuffer> sdkFuture = new CompletableFuture<>();
        prepared.whenComplete((result, failure) -> {
            if (failure != null) {
                Throwable cause = failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
                sdkFuture.completeExceptionally(SdkClientException.create("Unable to execute HTTP request: " + cause.getMessage(), cause));
            } else {
                sdkFuture.complete(result);
            }
        });
        return sdkFuture;
    }
}
