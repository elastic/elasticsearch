/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
