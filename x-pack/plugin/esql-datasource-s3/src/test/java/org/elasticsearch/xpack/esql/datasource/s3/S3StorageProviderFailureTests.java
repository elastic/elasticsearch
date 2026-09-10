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
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.StorageProviderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;

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

    // -------------------------------------------------------------------------
    // HeadBucket region-discovery retry tests
    // -------------------------------------------------------------------------

    /**
     * When a custom-endpoint {@code exists()} call fails with {@code AuthorizationHeaderMalformed}
     * the provider should issue a {@code HeadBucket} to discover the correct region, rebuild the
     * client, and retry the original {@code HeadObject} — which then succeeds.
     */
    public void testWrongRegionResolvesViaHeadBucketOnExists() throws IOException {
        S3Client wrongRegionClient = mock(S3Client.class);
        S3Client correctRegionClient = mock(S3Client.class);

        // First HeadObject fails with AuthorizationHeaderMalformed.
        when(wrongRegionClient.headObject(any(HeadObjectRequest.class))).thenThrow(
            s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1")
        );
        // HeadBucket also fails (wrong region) but carries the correct region in the response header.
        when(wrongRegionClient.headBucket(any(HeadBucketRequest.class))).thenThrow(
            s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1")
        );
        // Retry with correct client succeeds.
        when(correctRegionClient.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(42L).build()
        );

        S3StorageProvider provider = retryCapableProvider(wrongRegionClient, correctRegionClient);
        assertTrue(provider.exists(PATH));

        verify(wrongRegionClient).headBucket(any(HeadBucketRequest.class));
        verify(correctRegionClient).headObject(any(HeadObjectRequest.class));
    }

    /**
     * {@code SignatureDoesNotMatch} (wrong credentials) must NOT trigger a HeadBucket
     * region-discovery attempt — only {@code AuthorizationHeaderMalformed} should.
     */
    public void testSignatureDoesNotMatchDoesNotTriggerHeadBucket() {
        S3Client client = mock(S3Client.class);
        S3Exception sigFailure = s3FailureWithErrorCode(400, "SignatureDoesNotMatch", null);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(sigFailure);

        S3StorageProvider provider = retryCapableProvider(client, null);
        IOException thrown = expectThrows(IOException.class, () -> provider.exists(PATH));

        verify(client, never()).headBucket(any(HeadBucketRequest.class));
        assertThat(thrown.getCause(), instanceOf(S3Exception.class));
        assertSame(sigFailure, thrown.getCause());
    }

    /**
     * When a listing call fails with {@code AuthorizationHeaderMalformed} the iterator should
     * discover the correct region via HeadBucket, rebuild the client, and re-attempt the listing.
     */
    public void testWrongRegionResolvesViaHeadBucketOnListObjects() throws IOException {
        S3Client wrongRegionClient = mock(S3Client.class);
        S3Client correctRegionClient = mock(S3Client.class);

        when(wrongRegionClient.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(
            s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1")
        );
        when(wrongRegionClient.headBucket(any(HeadBucketRequest.class))).thenThrow(
            s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1")
        );
        when(correctRegionClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            ListObjectsV2Response.builder().contents(java.util.List.of()).isTruncated(false).build()
        );

        S3StorageProvider provider = retryCapableProvider(wrongRegionClient, correctRegionClient);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            assertFalse(iterator.hasNext());
        }

        verify(wrongRegionClient).headBucket(any(HeadBucketRequest.class));
        verify(correctRegionClient).listObjectsV2(any(ListObjectsV2Request.class));
    }

    /**
     * When the HeadBucket response carries no {@code x-amz-bucket-region} header (e.g. a store
     * that rejects wrong-region signing but does not implement that header), the provider must
     * not retry and must propagate the original IOException with the region hint.
     */
    public void testHeadBucketWithNoRegionHeaderPropagatesOriginalError() {
        S3Client client = mock(S3Client.class);
        S3Exception authMalformed = s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", null /* no region hint */);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(authMalformed);
        // HeadBucket also returns AuthorizationHeaderMalformed but carries no x-amz-bucket-region.
        when(client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", null));

        S3StorageProvider provider = retryCapableProvider(client, null);
        IOException thrown = expectThrows(IOException.class, () -> provider.exists(PATH));

        verify(client).headBucket(any(HeadBucketRequest.class));
        // buildRetryClient must not be called (retryCapableProvider throws AssertionError if it is)
        assertThat(thrown.getCause(), instanceOf(S3Exception.class));
        assertSame(authMalformed, thrown.getCause());
        assertThat(thrown.getMessage(), org.hamcrest.Matchers.containsString("set [region] on the dataset"));
    }

    /**
     * When region-discovery retry is disabled — either because an explicit region was configured
     * (wrong region stays an error) or because we are on the standard-S3 path where the SDK's
     * {@code crossRegionAccessEnabled} handles bucket-region redirect — an
     * {@code AuthorizationHeaderMalformed} error must propagate unchanged without issuing a
     * {@code HeadBucket} probe.
     */
    public void testAuthorizationHeaderMalformedWithRetryDisabledPropagatesError() {
        S3Client client = mock(S3Client.class);
        S3Exception authMalformed = s3FailureWithErrorCode(400, "AuthorizationHeaderMalformed", "eu-west-1");
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(authMalformed);

        // forTesting sets credentials = null, so shouldAttemptRegionRetry() returns false.
        // This covers two production scenarios:
        // 1. config.region() != null — explicit region set; wrong-region 400 must not be silently
        // retried because the caller deliberately pinned a region.
        // 2. config.endpoint() == null — standard S3 path; crossRegionAccessEnabled lets the SDK
        // redirect transparently, so our HeadBucket retry must not fire.
        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        IOException thrown = expectThrows(IOException.class, () -> provider.exists(PATH));

        verify(client, never()).headBucket(any(HeadBucketRequest.class));
        assertSame(authMalformed, thrown.getCause());
    }

    /**
     * Returns a provider with retry enabled that uses {@code initialClient} for the first attempt
     * and switches to {@code retryClient} after a successful HeadBucket region discovery.
     * Both {@link S3StorageProvider#shouldAttemptRegionRetry()} and
     * {@link S3StorageProvider#buildRetryClient(String)} are overridden so the test does not
     * need a real {@link S3Configuration} or credential provider.
     */
    private static S3StorageProvider retryCapableProvider(S3Client initialClient, S3Client retryClient) {
        return new S3StorageProvider(initialClient, null, (CustomWebIdentityTokenCredentialsProvider) null) {
            @Override
            boolean shouldAttemptRegionRetry() {
                return true;
            }

            @Override
            S3Client buildRetryClient(String region) {
                if (retryClient == null) {
                    throw new AssertionError("buildRetryClient should not have been called");
                }
                return retryClient;
            }
        };
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
}
