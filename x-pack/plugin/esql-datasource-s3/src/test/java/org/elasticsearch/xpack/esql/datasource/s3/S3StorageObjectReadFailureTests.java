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
import software.amazon.awssdk.services.s3.model.IntelligentTieringAccessTier;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
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

    /** What S3 answers when a key policy explicitly denies the reading principal {@code kms:Decrypt} on an SSE-KMS object. */
    private static final String KMS_DENIAL = "User: arn:aws:sts::123456789012:assumed-role/reader/session is not authorized to perform: "
        + "kms:Decrypt on resource: arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555 "
        + "with an explicit deny in a resource-based policy";

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
        assertNull("the S3 SDK cause must not reach caused_by", classified.getCause());
        assertEquals(thrown.getMessage(), classified.getMessage());
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
        assertSame(missing, ex.getCause());
    }

    /**
     * An SSE-KMS object read by a principal denied {@code kms:Decrypt} on its key: S3 answers 403 {@code AccessDenied} and
     * names the refused action in its message. The remedy is that permission, not new credentials or anonymous access,
     * and the principal and key ARNs from S3's message are not repeated.
     */
    public void testKmsDecryptDeniedNamesThePermissionNotTheCredentials() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception denied = s3Error(403, "AccessDenied", KMS_DENIAL);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(denied);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
        assertEquals(Condition.ACCESS_DENIED, ex.condition());
        assertThat(ex.getMessage(), containsString("not authorized to perform [kms:Decrypt]"));
        assertThat(ex.getMessage(), containsString("encrypted with a KMS key that principal cannot use. Allow it on that key"));
        assertThat(ex.getMessage(), containsString("HTTP 403 AccessDenied"));
        assertNoCredentialsRemedy(ex.getMessage());
        assertThat(ex.getMessage(), not(containsString("arn:aws:kms")));
        assertThat(ex.getMessage(), not(containsString("arn:aws:sts")));
        assertSame(denied, ex.getCause());
    }

    public void testDeniedActionOtherThanKmsNamesTheAction() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception denied = s3Error(
            403,
            "AccessDenied",
            "User: arn:aws:iam::123456789012:user/reader is not authorized to perform: s3:GetObject on resource: "
                + "\"arn:aws:s3:::test-bucket/data/file.parquet\" because no identity-based policy allows the s3:GetObject action"
        );
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(denied);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
        assertEquals(Condition.ACCESS_DENIED, ex.condition());
        assertThat(ex.getMessage(), containsString("not authorized to perform [s3:GetObject]. Allow it for that principal"));
        assertThat(ex.getMessage(), not(containsString("KMS")));
        assertThat(ex.getMessage(), not(containsString("arn:aws:")));
        assertThat(ex.getMessage(), not(containsString(BUCKET)));
        assertNoCredentialsRemedy(ex.getMessage());
    }

    /**
     * A 403 whose message names no action -- what a refused anonymous request and S3-compatible stores answer -- gets a
     * remedy that holds for every auth mode: it names no setting and does not suggest anonymous access.
     */
    public void testAccessDeniedNamingNoActionIsAuthModeAgnostic() {
        S3Client mockS3 = mock(S3Client.class);
        S3Exception denied = s3Error(403, "AccessDenied", "Access Denied");
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(denied);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
        assertEquals(Condition.ACCESS_DENIED, ex.condition());
        assertThat(ex.getMessage(), containsString("Verify that the data source is allowed to read this object"));
        assertNoCredentialsRemedy(ex.getMessage());
        assertSame(denied, ex.getCause());
    }

    /** S3's own verdict that the credentials are not valid: an unknown key id or a signature that does not match. */
    public void testRejectedCredentialsSayTheStoreDidNotAcceptThem() {
        for (String code : new String[] { "InvalidAccessKeyId", "SignatureDoesNotMatch" }) {
            S3Client mockS3 = mock(S3Client.class);
            when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(s3Error(403, code));

            S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
            ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
            assertEquals(Condition.ACCESS_DENIED, ex.condition());
            assertThat(ex.getMessage(), containsString("HTTP 403 " + code));
            assertThat(ex.getMessage(), containsString("did not accept the credentials the data source is configured with"));
            assertNoCredentialsRemedy(ex.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public void testKmsDecryptDeniedOnAsyncRead() throws Exception {
        S3AsyncClient mockAsyncS3 = asyncClientFailingWith(s3Error(403, "AccessDenied", KMS_DENIAL));
        Throwable thrown = readAsyncFailure(mockAsyncS3, 10);

        assertThat(thrown, instanceOf(ExternalClientException.class));
        assertEquals(Condition.ACCESS_DENIED, ((ExternalClientException) thrown).condition());
        assertThat(thrown.getMessage(), containsString("[kms:Decrypt]"));
        assertThat(thrown.getMessage(), not(containsString("arn:aws:")));
        assertNoCredentialsRemedy(thrown.getMessage());
        // Standard does not retry a 403.
        verify(mockAsyncS3, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
    }

    /**
     * An object in an archive storage class is refused with 403 {@code InvalidObjectState} until it is restored. That is
     * reported as such, naming the storage class, and the metadata fetch does not retry it as a range GET, since every
     * GET is refused the same way.
     */
    public void testArchivedObjectIsNotReportedAsAccessDenied() {
        S3Client mockS3 = mock(S3Client.class);
        InvalidObjectStateException archived = archived(StorageClass.GLACIER, null);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(archived);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::length);
        assertEquals(Condition.OBJECT_ARCHIVED, ex.condition());
        assertEquals(
            "External data object [file.parquet] is archived (HTTP 403 InvalidObjectState). It is in storage class [GLACIER]; "
                + "restore it, or wait for a restore in progress to finish, before reading it.",
            ex.getMessage()
        );
        assertSame(archived, ex.getCause());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(ExternalFailures.classify(ex)));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /** A HEAD refused as archived is not retried as a range GET either. */
    public void testArchivedObjectOnHeadFallback() {
        S3Client mockS3 = mock(S3Client.class);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(s3Error(500, "InternalError"));
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(archived(StorageClass.GLACIER, null));

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::length);
        assertEquals(Condition.OBJECT_ARCHIVED, ex.condition());
        assertThat(ex.getMessage(), containsString("storage class [GLACIER]"));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
    }

    public void testArchivedObjectOnExists() {
        S3Client mockS3 = mock(S3Client.class);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            archived(StorageClass.INTELLIGENT_TIERING, IntelligentTieringAccessTier.DEEP_ARCHIVE_ACCESS)
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::exists);
        assertEquals(Condition.OBJECT_ARCHIVED, ex.condition());
        assertThat(ex.getMessage(), containsString("storage class [INTELLIGENT_TIERING] and access tier [DEEP_ARCHIVE_ACCESS]"));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    /** A refusal carrying the code but no storage class still reads as archived, with the remedy alone. */
    public void testArchivedObjectWithoutTierOnNewStream() {
        S3Client mockS3 = mock(S3Client.class);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(archived(null, null));

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException ex = expectThrows(ExternalClientException.class, obj::newStream);
        assertEquals(Condition.OBJECT_ARCHIVED, ex.condition());
        assertThat(
            ex.getMessage(),
            containsString("(HTTP 403 InvalidObjectState). Restore it, or wait for a restore in progress to finish, before reading it.")
        );
        assertNoCredentialsRemedy(ex.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testArchivedObjectOnAsyncRead() throws Exception {
        S3AsyncClient mockAsyncS3 = asyncClientFailingWith(archived(StorageClass.DEEP_ARCHIVE, null));
        Throwable thrown = readAsyncFailure(mockAsyncS3, 10);

        assertThat(thrown, instanceOf(ExternalClientException.class));
        assertEquals(Condition.OBJECT_ARCHIVED, ((ExternalClientException) thrown).condition());
        assertThat(thrown.getMessage(), containsString("storage class [DEEP_ARCHIVE]"));
        // Standard does not retry a 403.
        verify(mockAsyncS3, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
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

    private static S3Exception s3Error(int status, String errorCode, String errorMessage) {
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message(errorMessage)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).errorMessage(errorMessage).build())
            .build();
    }

    /** The exception the SDK builds for a 403 {@code InvalidObjectState}, carrying what the error body reported. */
    private static InvalidObjectStateException archived(StorageClass storageClass, IntelligentTieringAccessTier accessTier) {
        return InvalidObjectStateException.builder()
            .storageClass(storageClass)
            .accessTier(accessTier)
            .statusCode(403)
            .message("The operation is not valid for the object's storage class")
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("InvalidObjectState")
                    .errorMessage("The operation is not valid for the object's storage class")
                    .build()
            )
            .build();
    }

    private static void assertNoCredentialsRemedy(String message) {
        assertThat(message, not(containsString("access_key")));
        assertThat(message, not(containsString("secret_key")));
        assertThat(message, not(containsString("auth=anonymous")));
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
