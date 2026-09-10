/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for S3StorageObject async read paths and dual-client wiring.
 */
@ThreadLeakFilters(filters = { S3StorageObjectAsyncTests.DelaySchedulerThreadFilter.class })
public class S3StorageObjectAsyncTests extends ESTestCase {

    /**
     * Suppresses the JDK's delay-scheduler daemon thread created by {@code CompletableFuture.delayedExecutor}.
     * The thread name changed across JDK versions: before JDK 25 it was {@code "CompletableFutureDelayScheduler"}
     * (a dedicated {@code ScheduledThreadPoolExecutor}); from JDK 25 it is {@code "ForkJoinPool.commonPool-delayScheduler"}
     * (the common pool's built-in {@code DelayScheduler}). This project targets JDK 25, so only the latter name applies.
     */
    public static class DelaySchedulerThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().equals("ForkJoinPool.commonPool-delayScheduler");
        }
    }

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);
    private static final byte[] PAYLOAD = "Hello from S3 async".getBytes(StandardCharsets.UTF_8);

    /**
     * AWS Standard retry semantics (same classification and attempt budget as production) but with
     * immediate backoff so retry tests do not sleep.
     */
    private static final RetryStrategy RETRY_STRATEGY = AwsRetryStrategy.standardRetryStrategy()
        .toBuilder()
        .backoffStrategy(BackoffStrategy.retryImmediately())
        .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())
        .build();

    private final S3Client mockSyncClient = mock(S3Client.class);
    private final S3AsyncClient mockAsyncClient = mock(S3AsyncClient.class);

    public void testSupportsNativeAsyncWithAsyncClient() {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);
        assertTrue(obj.supportsNativeAsync());
        assertTrue(obj.readBytesAsyncReleasesExecutor());
    }

    public void testSupportsNativeAsyncWithoutAsyncClient() {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, BUCKET, KEY, PATH);
        assertFalse(obj.supportsNativeAsync());
        assertFalse(obj.readBytesAsyncReleasesExecutor());
    }

    /**
     * {@code readBytesAsync} forwards to {@code startReadBytesAsync}. The sync-only fallback must
     * call the interface default {@code readBytesAsync}, not {@code super.startReadBytesAsync},
     * or the two methods recurse until the stack overflows.
     */
    public void testStartReadBytesAsyncWithoutAsyncClientDoesNotRecurse() throws Exception {
        when(mockSyncClient.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException("sync get"));

        S3StorageObject obj = new S3StorageObject(mockSyncClient, BUCKET, KEY, PATH);
        CountDownLatch latch = new CountDownLatch(1);
        try {
            obj.startReadBytesAsync(0, 1, FACTORY, Runnable::run, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer buffer) {
                    buffer.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
        } catch (StackOverflowError e) {
            fail("sync-only S3StorageObject must not recurse between readBytesAsync and startReadBytesAsync");
        }
        assertTrue("default async fallback must complete the listener", latch.await(5, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncHappyPath() throws Exception {
        GetObjectResponse response = GetObjectResponse.builder()
            .contentRange("bytes 0-18/19")
            .contentLength((long) PAYLOAD.length)
            .lastModified(Instant.parse("2026-04-01T12:00:00Z"))
            .build();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(
            invocation -> completeTransformer(invocation.getArgument(1), response, PAYLOAD)
        );

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();

        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                result.set(buffer);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        try (DirectReadBuffer drb = result.get()) {
            assertFalse("readBytesAsync must return a heap ByteBuffer", drb.buffer().isDirect());
            byte[] bytes = new byte[drb.buffer().remaining()];
            drb.buffer().get(bytes);
            assertArrayEquals(PAYLOAD, bytes);
        }
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncCachesMetadata() throws Exception {
        Instant lastModified = Instant.parse("2026-04-01T12:00:00Z");
        GetObjectResponse response = GetObjectResponse.builder().contentRange("bytes 0-18/1024").lastModified(lastModified).build();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(
            invocation -> completeTransformer(invocation.getArgument(1), response, PAYLOAD)
        );

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> latch.countDown(), e -> fail()));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(1024L, obj.length());
        assertEquals(lastModified, obj.lastModified());
    }

    /**
     * An S3-compatible store that does not implement If-Match on GET answers {@code NotImplemented}.
     * The retry omits the unsupported header but validates the response ETag against the retained pin.
     */
    @SuppressWarnings("unchecked")
    public void testAsyncIfMatchNotImplementedFallsBackUnpinned() throws Exception {
        GetObjectResponse response = GetObjectResponse.builder()
            .contentRange("bytes 0-18/19")
            .contentLength((long) PAYLOAD.length)
            .eTag("\"gen-1\"")
            .build();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            GetObjectRequest request = invocation.getArgument(0);
            if (request.ifMatch() != null) {
                CompletableFuture<DirectReadBuffer> failed = new CompletableFuture<>();
                failed.completeExceptionally(notImplemented());
                return failed;
            }
            return completeTransformer(invocation.getArgument(1), response, PAYLOAD);
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch first = new CountDownLatch(1);
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            buf.close();
            first.countDown();
        }, e -> fail("first async GET should succeed: " + e)));
        assertTrue(first.await(5, TimeUnit.SECONDS));
        assertEquals("\"gen-1\"", obj.contentGeneration());

        CountDownLatch second = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            buf.close();
            second.countDown();
        }, e -> {
            error.set(e);
            second.countDown();
        }));
        assertTrue(second.await(5, TimeUnit.SECONDS));
        assertNull("an If-Match-unsupported answer must fall back to an unpinned GET", error.get());

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockAsyncClient, times(3)).getObject(captor.capture(), any(AsyncResponseTransformer.class));
        assertNull(captor.getAllValues().get(0).ifMatch());
        assertEquals("\"gen-1\"", captor.getAllValues().get(1).ifMatch());
        assertNull("compat fallback omits If-Match", captor.getAllValues().get(2).ifMatch());
        assertEquals("\"gen-1\"", obj.contentGeneration());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncSuccessfulResponseFromDifferentGenerationFailsClosed() throws Exception {
        AtomicReference<String> responseEtag = new AtomicReference<>("\"gen-1\"");
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            GetObjectResponse response = GetObjectResponse.builder()
                .contentRange("bytes 0-18/19")
                .contentLength((long) PAYLOAD.length)
                .eTag(responseEtag.get())
                .build();
            return completeTransformer(invocation.getArgument(1), response, PAYLOAD);
        });
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch first = new CountDownLatch(1);
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            buf.close();
            first.countDown();
        }, e -> fail("first async GET should succeed: " + e)));
        assertTrue(first.await(5, TimeUnit.SECONDS));

        responseEtag.set("\"gen-2\"");
        CountDownLatch second = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> fail("expected generation failure"), e -> {
            failure.set(e);
            second.countDown();
        }));
        assertTrue(second.await(5, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(ExternalObjectChangedException.class));
    }

    /**
     * A plain 400 is not evidence the store lacks If-Match — a malformed range or a bad signature is
     * also a 400. Unpinning on those would silently retry the request without the generation pin and
     * leave the rest of the query unpinned, so the failure must surface instead.
     */
    @SuppressWarnings("unchecked")
    public void testAsyncGenericBadRequestDoesNotDropThePin() throws Exception {
        GetObjectResponse response = GetObjectResponse.builder()
            .contentRange("bytes 0-18/19")
            .contentLength((long) PAYLOAD.length)
            .eTag("\"gen-1\"")
            .build();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            GetObjectRequest request = invocation.getArgument(0);
            if (request.ifMatch() != null) {
                CompletableFuture<DirectReadBuffer> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                    S3Exception.builder()
                        .statusCode(400)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("InvalidArgument").errorMessage("Bad Request").build())
                        .message("Bad Request")
                        .build()
                );
                return failed;
            }
            return completeTransformer(invocation.getArgument(1), response, PAYLOAD);
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, BUCKET, KEY, PATH);

        CountDownLatch first = new CountDownLatch(1);
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            buf.close();
            first.countDown();
        }, e -> fail("first async GET should succeed: " + e)));
        assertTrue(first.await(5, TimeUnit.SECONDS));

        CountDownLatch second = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buf -> {
            buf.close();
            second.countDown();
        }, e -> {
            error.set(e);
            second.countDown();
        }));
        assertTrue(second.await(5, TimeUnit.SECONDS));
        assertNotNull("a generic 400 must surface, not be retried unpinned", error.get());

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockAsyncClient, times(2)).getObject(captor.capture(), any(AsyncResponseTransformer.class));
        assertEquals("the pin is still sent", "\"gen-1\"", captor.getAllValues().get(1).ifMatch());
        assertEquals("the pin is kept", "\"gen-1\"", obj.contentGeneration());
    }

    private static S3Exception notImplemented() {
        return (S3Exception) S3Exception.builder()
            .statusCode(400)
            .awsErrorDetails(
                AwsErrorDetails.builder().errorCode("NotImplemented").errorMessage("A header you provided is not implemented").build()
            )
            .message("A header you provided is not implemented")
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testReadBytesAsyncNotFound() throws Exception {
        CompletableFuture<DirectReadBuffer> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(NoSuchKeyException.builder().statusCode(404).message("Not Found").build());

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(failedFuture);

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, 10, FACTORY, Runnable::run, new ActionListener<>() {
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
        assertThat(error.get(), instanceOf(IOException.class));
        assertThat(error.get().getMessage(), containsString("Object not found"));
    }

    public void testReadBytesAsyncNegativePositionFails() throws Exception {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(-1, 10, FACTORY, Runnable::run, new ActionListener<>() {
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
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("position must be non-negative"));
    }

    public void testReadBytesAsyncLengthExceedsIntMaxFails() throws Exception {
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, (long) Integer.MAX_VALUE + 1, FACTORY, Runnable::run, new ActionListener<>() {
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
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("must fit in an int"));
    }

    public void testBothClientsClosedOnProviderClose() throws IOException {
        S3Client syncClient = mock(S3Client.class);
        S3AsyncClient asyncClient = mock(S3AsyncClient.class);

        S3StorageProvider provider = S3StorageProvider.forTesting(syncClient, asyncClient);
        provider.close();

        verify(syncClient).close();
        verify(asyncClient).close();
    }

    public void testAsyncClientClosedEvenIfSyncCloseThrows() {
        S3Client syncClient = mock(S3Client.class);
        S3AsyncClient asyncClient = mock(S3AsyncClient.class);

        doThrow(new RuntimeException("sync close failed")).when(syncClient).close();

        S3StorageProvider provider = S3StorageProvider.forTesting(syncClient, asyncClient);
        expectThrows(RuntimeException.class, provider::close);

        verify(asyncClient).close();
    }

    /**
     * A retryable transport failure (an {@link IOException}, matching the AWS Standard strategy's
     * retry-on-IOException condition) must be retried by {@code readBytesAsync}'s own retry loop —
     * SDK retries are disabled on the async client — and, critically, each attempt must get a
     * <b>fresh</b> {@link KnownLengthAsyncResponseTransformer}: a transformer must never span
     * attempts (see its class javadoc for the stale-exceptionOccurred rationale).
     */
    @SuppressWarnings("unchecked")
    public void testRetryableFailureRetriesWithFreshTransformer() throws Exception {
        GetObjectResponse response = GetObjectResponse.builder().contentLength((long) PAYLOAD.length).build();
        List<KnownLengthAsyncResponseTransformer<GetObjectResponse>> transformers = new CopyOnWriteArrayList<>();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            transformers.add(transformer);
            if (transformers.size() == 1) {
                return failTransformer(transformer, new IOException("connection reset by peer"));
            }
            return completeTransformer(transformer, response, PAYLOAD);
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, ActionListener.wrap(buffer -> {
            result.set(buffer);
            latch.countDown();
        }, e -> fail("read should have been retried and succeeded, but failed with: " + e)));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("one attempt plus one retry", 2, transformers.size());
        assertNotSame("each attempt must use a fresh transformer", transformers.get(0), transformers.get(1));
        try (DirectReadBuffer drb = result.get()) {
            byte[] bytes = new byte[drb.buffer().remaining()];
            drb.buffer().get(bytes);
            assertArrayEquals(PAYLOAD, bytes);
        }
    }

    /**
     * The production sequence from the review of the cross-attempt race: attempt N fails through its
     * subscriber's {@code onError}, the retry loop starts attempt N+1, and only then does netty's
     * stale {@code exceptionOccurred} for attempt N arrive — once with the throwable the subscriber
     * already handled and once with a fresh {@code IOException} from the channel-inactive teardown.
     * Because every attempt has its own transformer, the stale calls land on attempt N's (finished)
     * transformer and cannot fail attempt N+1's future or free its buffer: the in-flight attempt
     * completes successfully and no breaker charge leaks.
     */
    @SuppressWarnings("unchecked")
    public void testStaleExceptionOccurredFromFinishedAttemptCannotFailNextAttempt() throws Exception {
        CircuitBreaker breaker = new LimitedBreaker("stale-test", ByteSizeValue.ofMb(16));
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);
        GetObjectResponse response = GetObjectResponse.builder().contentLength((long) PAYLOAD.length).build();
        List<KnownLengthAsyncResponseTransformer<GetObjectResponse>> transformers = new CopyOnWriteArrayList<>();
        IOException attemptOneError = new IOException("attempt 1: connection reset mid-stream");

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            transformers.add(transformer);
            if (transformers.size() == 1) {
                // Attempt N: subscriber receives onError; this fails the attempt future, which is
                // what unblocks the retry — exactly the ordering netty produces.
                return failTransformer(transformer, attemptOneError);
            }
            // Attempt N+1: leave in flight; the test completes it after the stale calls fire.
            return transformer.prepare();
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, factory, Runnable::run, ActionListener.wrap(buffer -> {
            result.set(buffer);
            latch.countDown();
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));

        // The retry (attempt N+1) is already in flight. Now the stale notifications for attempt N
        // arrive: same-throwable redelivery, then the channel-inactive fresh IOException.
        assertEquals("retry should be in flight", 2, transformers.size());
        transformers.get(0).exceptionOccurred(attemptOneError);
        transformers.get(0).exceptionOccurred(new IOException("attempt 1: channel closed"));

        assertNull("stale exceptionOccurred must not have completed the read", failure.get());
        assertEquals(1, latch.getCount());

        // Attempt N+1 streams its payload; the read must succeed untouched by the stale calls.
        KnownLengthAsyncResponseTransformer<GetObjectResponse> second = transformers.get(1);
        second.onResponse(response);
        second.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {}

                    @Override
                    public void cancel() {}
                });
                s.onNext(ByteBuffer.wrap(PAYLOAD));
                s.onComplete();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull("read must not fail: " + failure.get(), failure.get());
        try (DirectReadBuffer drb = result.get()) {
            byte[] bytes = new byte[drb.buffer().remaining()];
            drb.buffer().get(bytes);
            assertArrayEquals(PAYLOAD, bytes);
        }
        assertEquals("no buffer charge may outlive the read", 0L, breaker.getUsed());
    }

    /** A non-retryable failure (a client-class S3 error) must fail after exactly one attempt. */
    @SuppressWarnings("unchecked")
    public void testNonRetryableFailureFailsWithoutRetry() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            calls.incrementAndGet();
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            return failTransformer(transformer, S3Exception.builder().statusCode(403).message("Access Denied").build());
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        obj.readBytesAsync(0, 10, FACTORY, Runnable::run, ActionListener.wrap(buffer -> fail("expected failure"), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("a 403 must not be retried", 1, calls.get());
        assertThat(error.get(), instanceOf(IOException.class));
    }

    /** When every attempt fails with a retryable error, the Standard budget (3 attempts) is honored. */
    @SuppressWarnings("unchecked")
    public void testRetryableFailureExhaustsAttemptsThenFails() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            int call = calls.incrementAndGet();
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            return failTransformer(transformer, new IOException("transient failure #" + call));
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        obj.readBytesAsync(0, 10, FACTORY, Runnable::run, ActionListener.wrap(buffer -> fail("expected failure"), e -> {
            error.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("Standard strategy allows 3 attempts", 3, calls.get());
        assertThat(error.get(), instanceOf(IOException.class));
        assertThat(error.get().getMessage(), containsString("Failed to read object from"));
    }

    /**
     * A throttling S3 response (429) carrying a {@code Retry-After} header must be retried, and
     * the header value must be extracted and passed to the retry strategy as the suggested delay.
     * This exercises the {@code retryAfterDelay} code path end-to-end.
     */
    @SuppressWarnings("unchecked")
    public void testThrottlingWithRetryAfterHeaderIsRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        GetObjectResponse response = GetObjectResponse.builder()
            .contentRange("bytes 0-18/19")
            .contentLength((long) PAYLOAD.length)
            .lastModified(Instant.parse("2026-04-01T12:00:00Z"))
            .build();

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            if (calls.incrementAndGet() == 1) {
                // First attempt: throttled with a Retry-After: 1 header (1000 ms suggested delay).
                S3Exception throttled = (S3Exception) S3Exception.builder()
                    .statusCode(429)
                    .message("Too Many Requests")
                    .awsErrorDetails(
                        AwsErrorDetails.builder()
                            .sdkHttpResponse(SdkHttpResponse.builder().statusCode(429).appendHeader("Retry-After", "1").build())
                            .build()
                    )
                    .build();
                return failTransformer(transformer, throttled);
            }
            return completeTransformer(transformer, response, PAYLOAD);
        });

        // Use immediate backoff so the test does not actually sleep for the suggested delay.
        RetryStrategy immediateBackoff = AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())
            .build();
        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, immediateBackoff, BUCKET, KEY, PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        obj.readBytesAsync(0, PAYLOAD.length, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                result.set(buffer);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("throttled request must be retried once", 2, calls.get());
        try (DirectReadBuffer buf = result.get()) {
            assertNotNull("read must succeed after retry", buf);
        }
    }

    /**
     * When {@link org.elasticsearch.core.Releasable#close} is called while a retry is waiting out
     * its backoff delay, the listener must be notified immediately — not after the delay expires.
     * Before the fix, the delay future was not stored, so cancel() was a no-op on the already-
     * completed attempt future, and the listener was stranded until the backoff timer fired.
     */
    @SuppressWarnings("unchecked")
    public void testCancelDuringBackoffNotifiesListenerPromptly() throws Exception {
        // A long fixed delay so the timer cannot fire before we assert the listener was called.
        RetryStrategy slowBackoff = AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .backoffStrategy(attempt -> Duration.ofSeconds(2))
            .throttlingBackoffStrategy(attempt -> Duration.ofSeconds(2))
            .build();

        // Single retryable failure: triggers the backoff after attempt 1.
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            return failTransformer(transformer, new IOException("connection reset"));
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, slowBackoff, BUCKET, KEY, PATH);

        CountDownLatch listenerCalled = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        Releasable cancel = obj.startReadBytesAsync(0, 10, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                listenerCalled.countDown();
            }
        });

        // Attempt 1 has failed and the retry is waiting in backoff.
        // Cancel must complete the listener immediately, not after the delay.
        cancel.close();

        assertTrue("listener must be notified promptly on cancel, not after the backoff delay", listenerCalled.await(1, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    /**
     * When {@link org.elasticsearch.core.Releasable#close} is called while a {@code getObject}
     * request is in flight, the listener must be notified once the SDK future is cancelled.
     */
    @SuppressWarnings("unchecked")
    public void testCancelInFlightNotifiesListener() throws Exception {
        CountDownLatch requestStarted = new CountDownLatch(1);

        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            CompletableFuture<DirectReadBuffer> future = transformer.prepare();
            requestStarted.countDown();
            // Return without completing the future, simulating an in-flight request.
            return future;
        });

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, RETRY_STRATEGY, BUCKET, KEY, PATH);

        CountDownLatch listenerCalled = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        Releasable cancel = obj.startReadBytesAsync(0, 10, FACTORY, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                listenerCalled.countDown();
            }
        });

        assertTrue("request must start", requestStarted.await(5, TimeUnit.SECONDS));

        // Cancel while the getObject future is pending.
        cancel.close();

        // FutureUtils.cancel completes the future with CancellationException, which flows to the listener.
        assertTrue("listener must be notified after in-flight cancel", listenerCalled.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    /**
     * If the user-supplied executor rejects the task when the backoff timer fires, the listener
     * must be notified with the rejection error rather than being silently stranded.
     */
    @SuppressWarnings("unchecked")
    public void testRejectedExecutorDuringBackoffNotifiesListener() throws Exception {
        // Short delay so the timer fires quickly in the test.
        RetryStrategy shortDelayStrategy = AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .backoffStrategy(attempt -> Duration.ofMillis(50))
            .throttlingBackoffStrategy(attempt -> Duration.ofMillis(50))
            .build();

        // Always fail with a retryable IOException so attempt 1 triggers the backoff.
        when(mockAsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = invocation.getArgument(1);
            return failTransformer(transformer, new IOException("connection reset"));
        });

        // An executor that rejects every task — simulates a shut-down thread pool.
        Executor rejectingExecutor = cmd -> { throw new RejectedExecutionException("executor shut down"); };

        S3StorageObject obj = new S3StorageObject(mockSyncClient, mockAsyncClient, shortDelayStrategy, BUCKET, KEY, PATH);

        CountDownLatch listenerCalled = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        // The initial attempt runs inline (initial delay = 0); after failure the 50 ms backoff is
        // scheduled. When the timer fires, rejectionSafeExecutor catches the RejectedExecutionException
        // and completes the listener rather than stranding it.
        obj.readBytesAsync(0, 10, FACTORY, rejectingExecutor, ActionListener.wrap(buf -> fail("expected failure"), e -> {
            error.set(e);
            listenerCalled.countDown();
        }));

        assertTrue("listener must be notified on executor rejection", listenerCalled.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(Exception.class));
    }

    /**
     * Drives {@code transformer} through the failure path the SDK/netty produce for a transport
     * error: the stream is wired and the subscriber's {@code onError} carries the failure, which
     * completes (fails) the attempt future.
     */
    private static CompletableFuture<DirectReadBuffer> failTransformer(
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer,
        Throwable error
    ) {
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {}

                    @Override
                    public void cancel() {}
                });
                s.onError(error);
            }
        });
        return future;
    }

    private static CompletableFuture<DirectReadBuffer> completeTransformer(
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer,
        GetObjectResponse response,
        byte[] payload
    ) {
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
        transformer.onResponse(response);
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {}

                    @Override
                    public void cancel() {}
                });
                s.onNext(ByteBuffer.wrap(payload));
                s.onComplete();
            }
        });
        return future;
    }
}
