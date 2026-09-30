/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import io.netty.channel.ChannelException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLHandshakeException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3StorageObjectReadFailureTests extends ESTestCase {

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    /**
     * AWS Standard retry semantics (same classification and attempt budget as production) but with
     * immediate backoff so failure-mapping tests do not sleep while the strategy spends its budget.
     */
    private static final RetryStrategy RETRY_STRATEGY = AwsRetryStrategy.standardRetryStrategy()
        .toBuilder()
        .backoffStrategy(BackoffStrategy.retryImmediately())
        .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())
        .build();

    public void testBareIllegalStateExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertNull(eue.getCause());
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
        assertNull(eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
    }

    public void testSdkClientExceptionWrappingTransportIoExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IOException io = new IOException("The target server failed to respond");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", io);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertNull(eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(eue)));
    }

    public void testSdkClientExceptionWrappingConnectExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        ConnectException refused = new ConnectException("Connection refused");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", refused);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertNull(eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(ExternalFailures.classify(eue)));
    }

    public void testSdkClientExceptionWrappingTimeoutExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        TimeoutException timeout = new TimeoutException("Read timed out");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", timeout);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertNull(eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testSdkClientExceptionWrappingNettyChannelExceptionIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        ChannelException nettyTimeout = new ChannelException("read timed out");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", nettyTimeout);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::newStream);
        assertNull(eue.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(eue));
        assertFalse(eue.throttling());
    }

    public void testSdkClientExceptionWithoutIoCauseStaysClientError() {
        S3Client mockS3 = mock(S3Client.class);
        SdkClientException credentials = SdkClientException.create("Could not load credentials");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(credentials);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertSame(credentials, thrown.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testCredentialChainSdkClientExceptionWithIoStaysClientError() {
        S3Client mockS3 = mock(S3Client.class);
        IOException io = new IOException("Failed to connect to service endpoint");
        SdkClientException inner = SdkClientException.create("Failed to load credentials from IMDS", io);
        SdkClientException outer = SdkClientException.create("Unable to load credentials from any provider", inner);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(outer);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertSame(outer, thrown.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testSdkClientExceptionWrappingUnknownHostStaysClientError() {
        S3Client mockS3 = mock(S3Client.class);
        UnknownHostException dns = new UnknownHostException("no-such-host.example.com");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", dns);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertSame(wrapped, thrown.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testSdkClientExceptionWrappingSslHandshakeStaysClientError() {
        S3Client mockS3 = mock(S3Client.class);
        SSLHandshakeException tls = new SSLHandshakeException("PKIX path building failed");
        SdkClientException wrapped = SdkClientException.create("Unable to execute HTTP request", tls);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(wrapped);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertSame(wrapped, thrown.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testExpiredTokenOnGetObjectIsTyped400() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception expired = s3Error(400, "ExpiredToken");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(expired);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, obj::newStream);
        assertSame(expired, thrown.getCause());
        assertThat(thrown.getMessage(), containsString("expired or invalid"));
        assertThat(thrown.getMessage(), containsString("Refresh the data source credentials"));
        // The storage path is intentionally omitted from the exception message.
        assertThat(thrown.getMessage(), not(containsString(PATH.toString())));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(thrown));
        RuntimeException classified = ExternalFailures.classify(thrown);
        assertThat(classified, instanceOf(ExternalCredentialsExpiredException.class));
        assertEquals(thrown.getMessage(), classified.getMessage());
        assertNull("the SDK exception stays with the storage layer", classified.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(classified));
    }

    public void testTokenRefreshRequiredOnGetObjectIsTyped400() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception expired = s3Error(400, "TokenRefreshRequired");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(expired);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, obj::newStream);
        assertSame(expired, thrown.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testExpiredTokenOnLengthSkipsMetadataFallbacks() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception expired = s3Error(400, "ExpiredToken");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(expired);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, obj::length);
        assertSame(expired, thrown.getCause());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    public void testExpiredTokenOn403MetadataDoesNotRangeGetFallback() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception expired = s3Error(403, "ExpiredToken");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(expired);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, obj::length);
        assertSame(expired, thrown.getCause());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    public void testGenericHttp400IsNotCredentialsExpiry() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception badRequest = (S3Exception) S3Exception.builder().statusCode(400).message("Bad Request").build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(badRequest);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertNull(ExceptionsHelper.unwrap(thrown, ExternalCredentialsExpiredException.class));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(thrown)));
    }

    public void testAuthorizationHeaderMalformedIsNotCredentialsExpiry() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception malformed = s3Error(400, "AuthorizationHeaderMalformed");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(malformed);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        IOException thrown = expectThrows(IOException.class, obj::newStream);
        assertNull(ExceptionsHelper.unwrap(thrown, ExternalCredentialsExpiredException.class));
        assertSame(malformed, thrown.getCause());
    }

    public void testNoSuchKeyIsObjectNotFound() {
        S3Client mockS3 = mock(S3Client.class);
        NoSuchKeyException missing = NoSuchKeyException.builder().statusCode(404).message("Not Found").build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(missing);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
        assertThat(ex.getMessage(), containsString("not found"));
        assertThat(ex.getMessage(), containsString(PATH.objectName()));
        assertNull(ex.getCause());
    }

    /**
     * esql-planning#2119: S3 answers a read refused by an IAM policy with a sentence naming the principal and the KMS
     * key ARNs Elasticsearch authenticated with. The 403 arm keeps the status, error code and remedy it composed and
     * does not chain the SDK exception, so no level of what the read boundary surfaces carries that sentence.
     */
    public void testAccessDeniedDoesNotChainTheProviderMessage() throws Exception {
        S3Exception denied = iamDenial();
        S3Client mockS3 = mock(S3Client.class);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(denied);
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException sync;
        S3StorageObject.ACCESS_DENIED_WARN.reset();
        try (MockLog mockLog = MockLog.capture(S3StorageObject.class)) {
            // The log is the only place the refusal's reason survives, so an operator must see it at default levels.
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                private boolean seen;

                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() == Level.WARN && event.getThrown() == denied) {
                        seen = true;
                    }
                }

                @Override
                public void assertMatched() {
                    assertTrue("the S3 denial, with its provider text, must be logged at WARN", seen);
                }
            });
            sync = expectThrows(ExternalClientException.class, obj::newStream);
            mockLog.assertAllExpectationsMatched();
        }
        try (MockLog mockLog = MockLog.capture(S3StorageObject.class)) {
            // Later denials within the interval stay at DEBUG, on any object: providers create one per file and split.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no second warn", S3StorageObject.class.getCanonicalName(), Level.WARN, "*")
            );
            expectThrows(ExternalClientException.class, obj::newStream);
            expectThrows(ExternalClientException.class, new S3StorageObject(mockS3, BUCKET, KEY, PATH)::newStream);
            mockLog.assertAllExpectationsMatched();
        }

        Throwable async = readAsyncFailure(asyncClientFailingWith(denied), 10);

        for (Throwable thrown : new Throwable[] { sync, async }) {
            assertThat(thrown, instanceOf(ExternalClientException.class));
            RuntimeException surfaced = ExternalFailures.classify(thrown);
            assertNoProviderText(thrown);
            assertNoProviderText(surfaced);
            assertThat(surfaced.getMessage(), containsString("HTTP 403"));
            assertThat(surfaced.getMessage(), containsString("AccessDenied"));
            assertThat(surfaced.getMessage(), containsString("Verify the access_key and secret_key"));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(surfaced));
        }
    }

    public void testRetryableStatusAndPreconditionDoNotChainTheProviderMessage() throws Exception {
        for (int status : new int[] { 503, 412 }) {
            S3Exception failure = (S3Exception) S3Exception.builder()
                .statusCode(status)
                .message(IAM_DENIAL)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("Refused").errorMessage(IAM_DENIAL).build())
                .build();
            S3Client mockS3 = mock(S3Client.class);
            when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(failure);
            S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
            RuntimeException thrown = expectThrows(RuntimeException.class, obj::newStream);
            assertNull("HTTP " + status + " must not chain the SDK exception", thrown.getCause());
            assertNoProviderText(ExternalFailures.classify(thrown));
        }
    }

    private static final String IAM_DENIAL = "User: arn:aws:sts::123456789012:assumed-role/reader/session is not authorized to "
        + "perform: kms:Decrypt on resource: arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555";

    private static S3Exception iamDenial() {
        return (S3Exception) S3Exception.builder()
            .statusCode(403)
            .message(IAM_DENIAL)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").errorMessage(IAM_DENIAL).build())
            .build();
    }

    private static void assertNoProviderText(Throwable surfaced) {
        for (Throwable t = surfaced; t != null; t = t.getCause() == t ? null : t.getCause()) {
            assertThat(String.valueOf(t.getMessage()), not(containsString("arn:aws:")));
            assertThat(String.valueOf(t.getMessage()), not(containsString("assumed-role")));
            for (Throwable suppressed : t.getSuppressed()) {
                assertThat(String.valueOf(suppressed.getMessage()), not(containsString("arn:aws:")));
            }
        }
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
        assertThat(thrown.getMessage(), containsString(PATH.objectName()));
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
            Condition.STORE_UNAVAILABLE,
            StoragePath.NONE,
            "",
            "",
            false,
            0L,
            new IOException("connection reset")
        );
        assertSame(withCause, readAsyncFailure(asyncClientFailingWith(withCause), 10));

        ExternalUnavailableException wrapped = new ExternalUnavailableException(
            Condition.STORE_UNAVAILABLE,
            StoragePath.NONE,
            "",
            "",
            false,
            0L
        );
        Throwable sdkWrapped = new CompletionException(SdkClientException.create("Unable to execute HTTP request", wrapped));
        assertSame(wrapped, readAsyncFailure(asyncClientFailingWith(sdkWrapped), 10));
    }

    public void testClosedClientOnLengthIsUnavailable503() {
        S3Client mockS3 = mock(S3Client.class);
        IllegalStateException ise = new IllegalStateException("Connection pool shut down");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(ise);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalUnavailableException eue = expectThrows(ExternalUnavailableException.class, obj::length);
        assertNull(eue.getCause());
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
        assertNull(eue.getCause());
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

    private static S3Exception s3Error(int status, String errorCode) {
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message(errorCode)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
            .build();
    }

    /** Reads {@code length} bytes through the native async path and returns the failure handed to the listener. */
    private static Throwable readAsyncFailure(S3AsyncClient mockAsyncS3, int length) throws InterruptedException {
        S3StorageObject obj = new S3StorageObject(mock(S3Client.class), mockAsyncS3, RETRY_STRATEGY, BUCKET, KEY, PATH);
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
