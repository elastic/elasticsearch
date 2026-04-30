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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Validates the exact number of S3 HEAD and GET requests made for metadata discovery.
 * Establishes baseline call counts, then verifies improvements after optimization.
 */
public class S3RequestCountingTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.parquet";
    private static final long FILE_SIZE = 100_000L;
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);
    private static final Instant LAST_MODIFIED = Instant.parse("2026-01-01T00:00:00Z");

    private final S3Client mockS3 = mock(S3Client.class);

    private void stubHeadResponse() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).build()
        );
    }

    private void stubSuffixRangeResponse() {
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes " + (FILE_SIZE - 1) + "-" + (FILE_SIZE - 1) + "/" + FILE_SIZE)
            .contentLength(1L)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0 })))
        );
    }

    /**
     * After optimization: length() uses suffix-range GET instead of HEAD.
     */
    public void testLengthTriggersOneSuffixRangeGet() throws IOException {
        stubSuffixRangeResponse();
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    /**
     * Calling length() twice should use the cached value (no second request).
     */
    public void testLengthCachesAfterFirstCall() throws IOException {
        stubSuffixRangeResponse();
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        obj.length();
        obj.length();

        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    /**
     * Creating S3StorageObject with pre-known length should make ZERO requests.
     */
    public void testPreKnownLengthSkipsHead() throws IOException {
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH, FILE_SIZE);

        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * The fix: using the SAME object for exists() and length() needs only ONE suffix-range GET.
     */
    public void testSameObjectExistsThenLengthCausesOneRequest() throws IOException {
        stubSuffixRangeResponse();

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(obj.exists());
        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    /**
     * Documents the anti-pattern fixed by the GlobExpander collapse: using two separate
     * S3StorageObject instances for exists() and length() wastes a request because
     * metadata is not shared across objects. See GlobExpander change in this PR.
     */
    public void testExistsThenNewObjectCausesTwoRequests() throws IOException {
        stubSuffixRangeResponse();

        S3StorageObject existsObj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(existsObj.exists());

        S3StorageObject readObj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        long length = readObj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, times(2)).getObject(any(GetObjectRequest.class));
    }

    /**
     * When the suffix-range GET fails with a non-403 error, fetchMetadata falls back to HEAD.
     */
    public void testSuffixRangeFailureFallsBackToHead() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(500).message("Internal Server Error").build()
        );
        stubHeadResponse();

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When the suffix-range GET returns 404 (NoSuchKeyException), the object is marked as not found.
     */
    public void testSuffixRangeNotFoundSetsNotFound() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().message("Not Found").build());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertFalse(obj.exists());
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When suffix-range GET returns 403, falls back to bytes=0-0 range GET (not HEAD,
     * since s3:GetObject covers both GET-range and HEAD — HEAD would also be denied).
     */
    public void testSuffixRange403FallsBackToRangeGet() throws IOException {
        // First call (suffix range) → 403; second call (bytes=0-0) → succeeds with Content-Range
        GetObjectResponse rangeResp = GetObjectResponse.builder()
            .contentRange("bytes 0-0/" + FILE_SIZE)
            .contentLength(1L)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        ).thenReturn(new ResponseInputStream<>(rangeResp, AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0 }))));

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, times(2)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When suffix-range GET returns 416 (Range Not Satisfiable), the object is empty (0 bytes).
     * No second request needed — 416 confirms existence with zero length.
     */
    public void testSuffixRange416MeansEmptyObject() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(416).message("Range Not Satisfiable").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(obj.exists());
        assertEquals(0L, obj.length());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When suffix-range GET succeeds but Content-Range is absent (unexpected for S3),
     * falls back to HEAD for the full metadata.
     */
    public void testMissingContentRangeFallsBackToHead() throws IOException {
        GetObjectResponse noContentRange = GetObjectResponse.builder().contentLength(1L).lastModified(LAST_MODIFIED).build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(noContentRange, AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0 })))
        );
        stubHeadResponse();

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        assertTrue(obj.exists());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
    }
}
