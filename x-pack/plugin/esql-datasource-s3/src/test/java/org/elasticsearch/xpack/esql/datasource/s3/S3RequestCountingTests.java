/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;
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
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).eTag("\"gen-1\"").build()
        );
    }

    private void stubFirstByteResponse() {
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes 0-0/" + FILE_SIZE)
            .contentLength(1L)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0 })))
        );
    }

    /**
     * The metadata lookup a resolve makes is one HeadObject and nothing else. No range GET: the caller is not going
     * to read the object, so it has no use for the generation pin a GET would establish, and HeadObject needs the
     * same s3:GetObject so it costs no more.
     */
    public void testObjectMetadataIsOneHeadAndNoGet() throws IOException {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).build()
        );

        StorageEntry metadata = new S3StorageObject(mockS3, BUCKET, KEY, PATH).headObjectMetadata();

        assertEquals(FILE_SIZE, metadata.length());
        assertEquals(LAST_MODIFIED, metadata.lastModified());
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * A refused HEAD carries no response body, so it has no S3 error code and cannot say which action was denied.
     * One range GET recovers the body, which is what makes the three denial conditions distinguishable. Asserting
     * the mapped condition AND the code matters: a 403 that reached the caller with the code stripped would still
     * satisfy expectThrows(Exception.class) while telling an operator nothing it could act on.
     */
    public void testObjectMetadataDenialRecoversTheErrorCode() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder()
                .statusCode(403)
                .message("Access Denied")
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").errorMessage("Access Denied").build())
                .build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException denied = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.ACCESS_DENIED, denied.condition());
        assertThat(denied.getMessage(), containsString("AccessDenied"));

        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    public void testLengthTriggersOneRangeGet() throws IOException {
        stubFirstByteResponse();
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        ArgumentCaptor<GetObjectRequest> sent = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3, times(1)).getObject(sent.capture());
        // The range is load-bearing: a 403 is surfaced without a second request because this already is the
        // cheapest read S3 serves, so widening it would quietly remove the fallback the 403 path gave up.
        assertEquals("bytes=0-0", sent.getValue().range());
    }

    /**
     * Calling length() twice should use the cached value (no second request).
     */
    public void testLengthCachesAfterFirstCall() throws IOException {
        stubFirstByteResponse();
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
     * Using the SAME object for exists() and length() needs only ONE range GET.
     */
    public void testSameObjectExistsThenLengthCausesOneRequest() throws IOException {
        stubFirstByteResponse();

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(obj.exists());
        long length = obj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
    }

    /**
     * newStream(pos, length) increments {@link StorageObjectMetrics} request counters. Close with
     * leftover at or below {@link TransientTypingInputStream#MAX_TRAILING_DRAIN_BYTES} drains the
     * remainder and books those received bytes.
     */
    public void testRangeNewStreamIncrementsMetrics() throws IOException {
        long rangeBytes = 1024L;
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes 0-" + (rangeBytes - 1) + "/" + FILE_SIZE)
            .contentLength(rangeBytes)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(new byte[(int) rangeBytes])))
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH, FILE_SIZE);
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        obj.attachMetrics(new ExternalSourceMetrics(registry), "s3");

        assertEquals(0L, obj.metrics().requestCount());
        obj.newStream(0, rangeBytes).close();

        StorageObjectMetrics metrics = obj.metrics();
        assertEquals(1L, metrics.requestCount());
        assertEquals(rangeBytes, metrics.bytesRead());
        assertEquals("close-with-no-read must publish drained leftover to APM", rangeBytes, apmBytesReadTotal(registry));
        assertTrue("requestNanos should be > 0", metrics.requestNanos() > 0);
        assertEquals(0L, metrics.retryCount());
    }

    public void testRangeNewStreamDrainThenAbortCountsReceivedBytes() throws IOException {
        long rangeBytes = 1024L;
        int drained = 17;
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes 0-" + (rangeBytes - 1) + "/" + FILE_SIZE)
            .contentLength(rangeBytes)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(new byte[(int) rangeBytes])))
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH, FILE_SIZE);
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        obj.attachMetrics(new ExternalSourceMetrics(registry), "s3");
        InputStream stream = obj.newStream(0, rangeBytes);
        assertEquals(drained, stream.read(new byte[drained]));
        obj.abortStream(stream);

        StorageObjectMetrics metrics = obj.metrics();
        assertEquals(1L, metrics.requestCount());
        assertEquals(drained, metrics.bytesRead());
        assertEquals("abort skips leftover; APM matches drained bytes", drained, apmBytesReadTotal(registry));
    }

    /**
     * Metadata-probe paths (length(), exists()) are intentionally NOT counted in metrics() —
     * they're not data reads.
     */
    public void testMetadataProbesDoNotCountAsRequests() throws IOException {
        stubFirstByteResponse();
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        obj.length();
        obj.exists();

        assertEquals(0L, obj.metrics().requestCount());
        assertEquals(0L, obj.metrics().bytesRead());
    }

    /**
     * Documents the anti-pattern fixed by the GlobExpander collapse: using two separate
     * S3StorageObject instances for exists() and length() wastes a request because
     * metadata is not shared across objects. See GlobExpander change in this PR.
     */
    public void testExistsThenNewObjectCausesTwoRequests() throws IOException {
        stubFirstByteResponse();

        S3StorageObject existsObj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(existsObj.exists());

        S3StorageObject readObj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        long length = readObj.length();

        assertEquals(FILE_SIZE, length);
        verify(mockS3, times(2)).getObject(any(GetObjectRequest.class));
    }

    /**
     * When the range GET fails with a non-403 error, probeObject falls back to HEAD.
     */
    public void testRangeGetFailureFallsBackToHead() throws IOException {
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
     * The HEAD fallback carries its own fallback back to a range GET, for a policy that grants s3:GetObject but
     * not s3:ListBucket: a range GET that failed for an unrelated reason, then a HEAD refused with 403, is still
     * answered by retrying the range GET.
     */
    public void testHeadFallbackDeniedFallsBackToRangeGet() throws IOException {
        GetObjectResponse resp = GetObjectResponse.builder()
            .contentRange("bytes 0-0/" + FILE_SIZE)
            .contentLength(1L)
            .lastModified(LAST_MODIFIED)
            .build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(500).message("Internal Server Error").build()
        ).thenReturn(new ResponseInputStream<>(resp, AbortableInputStream.create(new ByteArrayInputStream(new byte[] { 0 }))));
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);

        assertEquals(FILE_SIZE, obj.length());
        verify(mockS3, times(2)).getObject(any(GetObjectRequest.class));
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When the range GET returns 404 (NoSuchKeyException), the object is marked as not found.
     */
    public void testRangeGetNotFoundSetsNotFound() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().message("Not Found").build());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertFalse(obj.exists());
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * A 403 is answered in one request. This request already is the cheapest read, and a HEAD needs the same
     * s3:GetObject, so a second request would only be refused again.
     */
    public void testDenialCostsOneRequestAndNoHead() {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(403).message("Access Denied").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        // Assert the mapped type and condition, not merely that something threw: a 403 mapped to anything else
        // would still satisfy expectThrows(Exception.class) while telling the caller the wrong thing.
        ExternalClientException denied = expectThrows(ExternalClientException.class, obj::length);
        assertEquals(ExternalClientException.Condition.ACCESS_DENIED, denied.condition());

        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * A 416 means the object exists and is empty. One request covers it: the 416 is itself an
     * answer, and the probe stamps the timestamp as well as the length, so a following lastModified() does not
     * find it unset and run the whole probe again.
     */
    public void testRangeGet416MeansEmptyObject() throws IOException {
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(416).message("Range Not Satisfiable").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        assertTrue(obj.exists());
        assertEquals(0L, obj.length());
        // The timestamp matters as much as the length: unset, this call returns null for an object that exists.
        assertEquals(Instant.EPOCH, obj.lastModified());
        verify(mockS3, times(1)).getObject(any(GetObjectRequest.class));
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
    }

    /**
     * When the range GET succeeds but Content-Range is absent (unexpected for S3),
     * falls back to HEAD for the full metadata.
     */
    public void testMissingContentRangeFallsBackToHead() throws IOException {
        GetObjectResponse noContentRange = GetObjectResponse.builder()
            .contentLength(1L)
            .lastModified(LAST_MODIFIED)
            .eTag("\"gen-1\"")
            .build();
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

    private static long apmBytesReadTotal(RecordingMeterRegistry registry) {
        return registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }
}
