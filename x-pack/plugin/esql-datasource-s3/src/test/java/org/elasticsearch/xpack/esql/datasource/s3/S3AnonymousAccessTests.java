/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for S3 anonymous access (auth=none), HEAD fallback, and listing error handling.
 */
public class S3AnonymousAccessTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final long OBJECT_SIZE = 123456L;
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    private final S3Client mockS3Client = mock(S3Client.class);

    /**
     * When HeadObject returns 403, fetchMetadata should fall back to a range GET
     * and discover the object length from the Content-Range header.
     */
    public void testHeadFallbackToRangeGet() throws IOException {
        // HEAD returns 403
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );

        // Range GET succeeds with Content-Range
        GetObjectResponse getResponse = GetObjectResponse.builder()
            .contentRange("bytes 0-0/" + OBJECT_SIZE)
            .contentLength(1L)
            .lastModified(Instant.parse("2026-03-18T12:00:00Z"))
            .build();
        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
            getResponse,
            AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0x50 }))
        );
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

        S3StorageObject obj = new S3StorageObject(mockS3Client, BUCKET, KEY, PATH);

        assertEquals(OBJECT_SIZE, obj.length());
        assertTrue(obj.exists());
        assertEquals(Instant.parse("2026-03-18T12:00:00Z"), obj.lastModified());
    }

    /**
     * When HeadObject returns 403 and the range GET indicates the object doesn't exist,
     * the object should be marked as not found.
     */
    public void testHeadFallbackObjectNotFound() throws IOException {
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );

        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(
            NoSuchKeyException.builder().statusCode(404).message("Not Found").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3Client, BUCKET, KEY, PATH);

        assertFalse(obj.exists());
    }

    /**
     * When HeadObject returns a non-403 error, it should propagate as IOException
     * without attempting the range GET fallback.
     */
    public void testHeadNon403ErrorPropagates() {
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(500).message("Internal Server Error").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3Client, BUCKET, KEY, PATH);

        IOException e = expectThrows(IOException.class, obj::length);
        assertThat(e.getMessage(), containsString("HeadObject request failed"));
    }

    /**
     * When HeadObject succeeds normally, no fallback is needed.
     */
    public void testHeadSucceedsNormally() throws IOException {
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
            .contentLength(OBJECT_SIZE)
            .lastModified(Instant.parse("2026-03-18T12:00:00Z"))
            .build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headResponse);

        S3StorageObject obj = new S3StorageObject(mockS3Client, BUCKET, KEY, PATH);

        assertEquals(OBJECT_SIZE, obj.length());
        assertTrue(obj.exists());
    }

    /**
     * When HeadObject returns 403 and the range GET also fails (non-404), the error
     * should propagate with a descriptive message.
     */
    public void testHeadFallbackRangeGetAlsoFails() {
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );
        when(mockS3Client.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3Client, BUCKET, KEY, PATH);

        IOException e = expectThrows(IOException.class, obj::length);
        assertThat(e.getMessage(), containsString("HEAD denied, range GET also failed"));
    }

    /**
     * Verify S3Configuration correctly identifies anonymous mode.
     */
    public void testConfigurationAnonymousMode() {
        S3Configuration anonymous = S3Configuration.fromFields(null, null, "http://endpoint", "us-east-1", "none");
        assertTrue(anonymous.isAnonymous());
        assertFalse(anonymous.hasCredentials());

        S3Configuration credentials = S3Configuration.fromFields("ak", "sk", null, null);
        assertFalse(credentials.isAnonymous());
        assertTrue(credentials.hasCredentials());

        S3Configuration defaultChain = S3Configuration.fromFields(null, null, "http://endpoint", null);
        assertFalse(defaultChain.isAnonymous());
        assertFalse(defaultChain.hasCredentials());
    }

    /**
     * Verify that auth=none is mutually exclusive with credentials.
     */
    public void testConfigurationAnonymousModeConflictsWithCredentials() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> S3Configuration.fromFields("ak", "sk", null, null, "none"));
    }
}
