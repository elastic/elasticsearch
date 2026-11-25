/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.StorageProviderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.TestConnectionNotSupportedException;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3StorageProviderFailureTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("s3://test-bucket/data/file.parquet");
    private static final StoragePath PREFIX = StoragePath.of("s3://test-bucket/data");

    public void testExistsTypesEveryRetryableStatus() {
        for (int status : new int[] { 429, 500, 502, 503, 504 }) {
            S3Client client = mock(S3Client.class);
            S3Exception failure = s3Failure(status, status == 503 ? "3" : null);
            when(client.headObject(any(HeadObjectRequest.class))).thenThrow(failure);

            S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
            ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, () -> provider.exists(PATH));

            assertSame(failure, thrown.getCause());
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, thrown.status());
            assertEquals(ExternalUnavailableException.isThrottlingStatus(status), thrown.throttling());
            assertEquals(status == 503 ? 3000L : 0L, thrown.retryAfterMs());
        }
    }

    public void testExistsRangeFallbackTypesRetryableFailure() {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(s3Failure(403, null));
        S3Exception unavailable = s3Failure(503, null);
        when(client.getObject(any(GetObjectRequest.class))).thenThrow(unavailable);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, () -> provider.exists(PATH));

        assertSame(unavailable, thrown.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, thrown.status());
        assertTrue(thrown.throttling());
    }

    public void testLazyListingTypesEveryRetryableStatus() throws IOException {
        for (int status : new int[] { 429, 500, 502, 503, 504 }) {
            S3Client client = mock(S3Client.class);
            S3Exception failure = s3Failure(status, null);
            when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(failure);

            S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
            try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
                ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, iterator::hasNext);
                assertSame(failure, thrown.getCause());
                assertEquals(RestStatus.SERVICE_UNAVAILABLE, thrown.status());
                assertEquals(ExternalUnavailableException.isThrottlingStatus(status), thrown.throttling());
            }
        }
    }

    public void testLazyListingRetriesMappedFailureOnSameIterator() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(s3Failure(500, null))
            .thenReturn(ListObjectsV2Response.builder().contents(java.util.List.of()).isTruncated(false).build());

        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> S3StorageProvider.forTesting(client, null)));
            StorageProvider provider = registry.provider(PREFIX);
            try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
                assertFalse(iterator.hasNext());
            }
        }

        verify(client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testExistsTypesSdkClientTransportFailure() {
        S3Client client = mock(S3Client.class);
        SdkClientException failure = transportWrap();
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(failure);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, () -> provider.exists(PATH));

        assertSame(failure, thrown.getCause());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, thrown.status());
        assertFalse(thrown.throttling());
    }

    public void testLazyListingTypesSdkClientTransportFailure() throws IOException {
        S3Client client = mock(S3Client.class);
        SdkClientException failure = transportWrap();
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(failure);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, iterator::hasNext);
            assertSame(failure, thrown.getCause());
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, thrown.status());
            assertFalse(thrown.throttling());
        }
    }

    public void testLazyListingRetriesSdkClientTransportFailureOnSameIterator() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(transportWrap())
            .thenReturn(ListObjectsV2Response.builder().contents(java.util.List.of()).isTruncated(false).build());

        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> S3StorageProvider.forTesting(client, null)));
            StorageProvider provider = registry.provider(PREFIX);
            try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
                assertFalse(iterator.hasNext());
            }
        }

        verify(client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testListingExpiredTokenThrowsTypedException() throws IOException {
        S3Client client = mock(S3Client.class);
        S3Exception expired = s3FailureWithErrorCode(400, "ExpiredToken", null);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(expired);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, iterator::hasNext);
            assertSame(expired, thrown.getCause());
            assertThat(thrown.getMessage(), containsString("expired or invalid"));
            assertThat(thrown.getMessage(), containsString("Refresh the data source credentials"));
            assertEquals(RestStatus.BAD_REQUEST, thrown.status());
        }
        verify(client, never()).headBucket(any(HeadBucketRequest.class));
    }

    public void testExistsExpiredTokenThrowsTypedException() {
        S3Client client = mock(S3Client.class);
        S3Exception expired = s3FailureWithErrorCode(400, "ExpiredToken", null);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(expired);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        ExternalCredentialsExpiredException thrown = expectThrows(ExternalCredentialsExpiredException.class, () -> provider.exists(PATH));
        assertSame(expired, thrown.getCause());
        assertThat(thrown.getMessage(), containsString("expired or invalid"));
        verify(client, never()).headBucket(any(HeadBucketRequest.class));
    }

    public void testExpiredTokenDoesNotTriggerHeadBucket() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(s3FailureWithErrorCode(400, "ExpiredToken", null));
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(s3FailureWithErrorCode(400, "ExpiredToken", null));

        try (S3StorageProvider provider = S3StorageProvider.forTesting(client, null)) {
            expectThrows(ExternalCredentialsExpiredException.class, () -> provider.exists(PATH));
            try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
                expectThrows(ExternalCredentialsExpiredException.class, iterator::hasNext);
            }
            verify(client, never()).headBucket(any(HeadBucketRequest.class));
        }
    }

    public void testListingAuthorizationHeaderMalformedIsNotExpiry() throws IOException {
        S3Client client = mock(S3Client.class);
        S3Exception authMalformed = s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1");
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(authMalformed);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            UncheckedIOException thrown = expectThrows(UncheckedIOException.class, iterator::hasNext);
            assertNull(ExceptionsHelper.unwrap(thrown, ExternalCredentialsExpiredException.class));
            assertThat(thrown.getCause(), instanceOf(IOException.class));
            assertSame(authMalformed, thrown.getCause().getCause());
        }
    }

    /**
     * {@code SignatureDoesNotMatch} (wrong credentials) must NOT trigger a HeadBucket
     * region-discovery attempt — only {@code AuthorizationHeaderMalformed} should.
     */
    public void testSignatureDoesNotMatchDoesNotTriggerHeadBucket() throws IOException {
        S3Client client = mock(S3Client.class);
        S3Exception sigFailure = s3FailureWithErrorCode(400, "SignatureDoesNotMatch", null);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(sigFailure);

        try (S3StorageProvider provider = S3StorageProvider.forTesting(client, null)) {
            IOException thrown = expectThrows(IOException.class, () -> provider.exists(PATH));

            verify(client, never()).headBucket(any(HeadBucketRequest.class));
            assertThat(thrown.getCause(), instanceOf(S3Exception.class));
            assertSame(sigFailure, thrown.getCause());
        }
    }

    /**
     * Injected-client tests do not run {@link BucketRegionResolver}; a wrong-region 400 must
     * propagate without a HeadBucket probe.
     */
    public void testAuthorizationHeaderMalformedOnInjectedClientPropagatesError() {
        S3Client client = mock(S3Client.class);
        S3Exception authMalformed = s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1");
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(authMalformed);

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        IOException thrown = expectThrows(IOException.class, () -> provider.exists(PATH));

        verify(client, never()).headBucket(any(HeadBucketRequest.class));
        assertSame(authMalformed, thrown.getCause());
    }

    private static SdkClientException transportWrap() {
        return SdkClientException.create("Unable to execute HTTP request", new IOException("The target server failed to respond"));
    }

    /**
     * Builds an {@link S3Exception} with an explicit AWS error code and an optional
     * {@code x-amz-bucket-region} response header (used by HeadBucket region-discovery tests).
     */
    private static S3Exception s3FailureWithErrorCode(int status, String errorCode, String bucketRegionHeader) {
        SdkHttpResponse.Builder response = SdkHttpResponse.builder().statusCode(status);
        if (bucketRegionHeader != null) {
            response.appendHeader("x-amz-bucket-region", bucketRegionHeader);
        }
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message(errorCode)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).sdkHttpResponse(response.build()).build())
            .build();
    }

    private static S3Exception s3Failure(int status, String retryAfter) {
        SdkHttpResponse.Builder response = SdkHttpResponse.builder().statusCode(status);
        if (retryAfter != null) {
            response.appendHeader("Retry-After", retryAfter);
        }
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message("S3 failure")
            .awsErrorDetails(AwsErrorDetails.builder().sdkHttpResponse(response.build()).build())
            .build();
    }

    public void testTestConnectionAnonymousIsUntestable() {
        S3Configuration config = S3Configuration.fromFields(null, null, null, null, "anonymous");
        S3StorageProvider provider = new S3StorageProvider(config, null);
        TestConnectionNotSupportedException ex = expectThrows(TestConnectionNotSupportedException.class, provider::testConnection);
        assertThat(ex.getMessage(), containsString("anonymous"));
    }

    public void testTestConnectionAccessDeniedIsUntestable() {
        S3Client client = mock(S3Client.class);
        S3Exception accessDenied = (S3Exception) S3Exception.builder()
            .statusCode(403)
            .message("Access Denied")
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("AccessDenied")
                    .sdkHttpResponse(SdkHttpResponse.builder().statusCode(403).build())
                    .build()
            )
            .build();
        when(client.listBuckets()).thenThrow(accessDenied);
        // Non-anonymous config so the short-circuit does not fire; we test the 403-AccessDenied branch.
        S3Configuration config = S3Configuration.fromFields("key", "secret", null, null);
        S3StorageProvider provider = new S3StorageProvider(config, client);
        TestConnectionNotSupportedException ex = expectThrows(TestConnectionNotSupportedException.class, provider::testConnection);
        assertThat(ex.getMessage(), containsString("AccessDenied"));
    }

    public void testTestConnectionAuthorizationHeaderMalformedIsUntestable() {
        S3Client client = mock(S3Client.class);
        S3Exception authMalformed = s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1");
        when(client.listBuckets()).thenThrow(authMalformed);
        // Non-anonymous config so the anonymous short-circuit does not fire.
        S3Configuration config = S3Configuration.fromFields("key", "secret", null, null);
        S3StorageProvider provider = new S3StorageProvider(config, client);
        TestConnectionNotSupportedException ex = expectThrows(TestConnectionNotSupportedException.class, provider::testConnection);
        assertThat(ex.getMessage(), containsString("AuthorizationHeaderMalformed"));
    }
}
