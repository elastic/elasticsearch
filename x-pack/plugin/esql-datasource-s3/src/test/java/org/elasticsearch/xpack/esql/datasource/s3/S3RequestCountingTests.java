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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;

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
     * only that something threw would pass without the recovery, so this pins the mapped condition and the code.
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

    /**
     * An expired session token is identified by error code, and a HEAD carries none, so recovering the body is
     * what keeps the type. Without it the resolve path reports "HTTP 400" and the typed 400 the resolver
     * unwraps is lost.
     */
    public void testObjectMetadataRecoversAnExpiredTokenDeliveredAsA400() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(400).build());
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder()
                .statusCode(400)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("ExpiredToken").errorMessage("token expired").build())
                .build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        expectThrows(ExternalCredentialsExpiredException.class, obj::headObjectMetadata);
    }

    /**
     * A refused HEAD followed by a GET that answers NoSuchKey means the object is not there.
     * probeObjectViaRangeGet records that rather than throwing, so without an explicit check the method returns
     * length 0 and an EPOCH timestamp, and the resolve path caches that as the file's metadata.
     */
    public void testObjectMetadataOnAMissingObjectDoesNotReportAnEmptyOne() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(403).build());
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().message("Not Found").build());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException missing = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.OBJECT_NOT_FOUND, missing.condition());
    }

    /**
     * A HEAD on an archived object succeeds and names the class in a header, where a GET is refused with
     * InvalidObjectState. The resolve has to reach the same conclusion from the header, or it accepts the object
     * and the read fails later instead.
     */
    public void testObjectMetadataRefusesAnArchivedObject() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).storageClass("GLACIER").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException archived = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.OBJECT_ARCHIVED, archived.condition());
        assertThat(archived.getMessage(), containsString("GLACIER"));
        // The remedy, not just the condition: an archived message that does not say how to fix it is the regression
        // ExternalErrorSurfaceIT caught when the resolve stopped failing here.
        assertThat(archived.getMessage(), containsString("restore it"));

        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * A refused HEAD whose recovery GET reports 416 is an empty object, not a failure. probeObject already drew
     * that conclusion; the recovery path reported "HTTP 416" instead.
     */
    public void testObjectMetadataRecoveryTreatsA416AsAnEmptyObject() throws IOException {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(403).build());
        when(mockS3.getObject(any(GetObjectRequest.class))).thenThrow(
            S3Exception.builder().statusCode(416).message("Range Not Satisfiable").build()
        );

        StorageEntry metadata = new S3StorageObject(mockS3, BUCKET, KEY, PATH).headObjectMetadata();

        assertEquals(0L, metadata.length());
        assertEquals(Instant.EPOCH, metadata.lastModified());
    }

    /**
     * A restored object still names its archive class; {@code x-amz-restore} is what says the copy is available.
     * Refusing on the class alone tells the user to restore an object that is already restored.
     */
    public void testObjectMetadataAcceptsARestoredArchivedObject() throws IOException {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder()
                .contentLength(FILE_SIZE)
                .lastModified(LAST_MODIFIED)
                .storageClass("GLACIER")
                .restore("ongoing-request=\"false\", expiry-date=\"Fri, 21 Dec 2026 00:00:00 GMT\"")
                .build()
        );

        StorageEntry metadata = new S3StorageObject(mockS3, BUCKET, KEY, PATH).headObjectMetadata();

        assertEquals(FILE_SIZE, metadata.length());
    }

    /**
     * A restore still running is not a restore: the copy is not available yet, so a GET is still refused. Reading
     * only whether {@code x-amz-restore} is present accepts the object here and fails at read time instead, with a
     * message that no longer says to wait.
     */
    public void testObjectMetadataRefusesAnObjectWhoseRestoreIsStillRunning() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder()
                .contentLength(FILE_SIZE)
                .lastModified(LAST_MODIFIED)
                .storageClass("GLACIER")
                .restore("ongoing-request=\"true\"")
                .build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException archived = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.OBJECT_ARCHIVED, archived.condition());
        assertThat(archived.getMessage(), containsString("wait for a restore in progress"));

        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * An Intelligent-Tiering object names INTELLIGENT_TIERING as its class and carries the archive state in
     * {@code x-amz-archive-status}, so reading the class alone lets it through and the read fails later.
     */
    public void testObjectMetadataRefusesAnIntelligentTieringArchivedObject() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder()
                .contentLength(FILE_SIZE)
                .lastModified(LAST_MODIFIED)
                .storageClass("INTELLIGENT_TIERING")
                .archiveStatus("DEEP_ARCHIVE_ACCESS")
                .build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException archived = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.OBJECT_ARCHIVED, archived.condition());
        // Both parts, in the one form the refused-GET path also renders: the class alone does not say why this
        // object is archived, and a separator that drifts from the sibling is the divergence this pins.
        assertThat(archived.getMessage(), containsString("storage class [INTELLIGENT_TIERING] and access tier [DEEP_ARCHIVE_ACCESS]"));
        assertThat(archived.getMessage(), containsString("restore it"));
    }

    /**
     * A 404 is the other status the recovery GET is not spent on: a GET would only repeat it. The object is simply
     * not there, which is already the whole answer, and the recovery exists to learn an error code a 404 has.
     */
    public void testObjectMetadataDoesNotRecoverOnANotFound() {
        // statusCode must be set explicitly: the builder leaves it 0, which fails the guard's >= 400 arm before
        // the 404 exclusion this test is named for is ever reached, and the test would pass for the wrong reason.
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(
            NoSuchKeyException.builder().statusCode(404).message("Not Found").build()
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        ExternalClientException missing = expectThrows(ExternalClientException.class, obj::headObjectMetadata);
        assertEquals(ExternalClientException.Condition.OBJECT_NOT_FOUND, missing.condition());

        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * A throttled HEAD must not spend a second request recovering an error body: the retry layer is about to ask
     * again, so a GET per attempt is two requests against a store that asked for fewer.
     */
    public void testObjectMetadataDoesNotRecoverOnARetryableStatus() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenThrow(S3Exception.builder().statusCode(429).build());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        // The mapped condition matters as much as the absent GET: a 429 that stopped arriving as back-pressure
        // would still satisfy expectThrows(Exception.class) while the retry layer stopped backing off for it.
        ExternalUnavailableException throttled = expectThrows(ExternalUnavailableException.class, obj::headObjectMetadata);
        assertEquals(ExternalUnavailableException.Condition.STORE_THROTTLED, throttled.condition());
        assertTrue(throttled.throttling());

        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /** An instant-retrieval class is readable, so it must not be refused. */
    public void testObjectMetadataAcceptsAnInstantRetrievalClass() throws IOException {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).storageClass("GLACIER_IR").build()
        );

        StorageEntry metadata = new S3StorageObject(mockS3, BUCKET, KEY, PATH).headObjectMetadata();

        assertEquals(FILE_SIZE, metadata.length());
    }

    /** A HeadObject response without a Content-Length cannot answer the length the caller asked for. */
    public void testObjectMetadataWithoutContentLengthFails() {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().lastModified(LAST_MODIFIED).build());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        expectThrows(IOException.class, obj::headObjectMetadata);
    }

    /**
     * Through the provider, which is what the resolve path calls. The object-level tests above cannot see the
     * override being wired: deleting it leaves the interface default in place, which answers from a range GET.
     */
    public void testProviderObjectMetadataIsOneHeadAndNoGet() throws IOException {
        when(mockS3.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(FILE_SIZE).lastModified(LAST_MODIFIED).build()
        );
        S3StorageProvider provider = new S3StorageProvider(S3Configuration.fromMap(Map.of("auth", "anonymous")), mockS3);

        StorageEntry metadata = provider.objectMetadata(PATH);

        assertEquals(FILE_SIZE, metadata.length());
        assertEquals(LAST_MODIFIED, metadata.lastModified());
        verify(mockS3, times(1)).headObject(any(HeadObjectRequest.class));
        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * Pre-known length and modification time reach the handle, so nothing asks the store for what the caller
     * already supplied. The four single-path entry points resolve the bucket's clients through one step and then
     * construct at different arities, and an argument dropped on the way to the longest one is otherwise silent.
     */
    public void testProviderPassesPreKnownLengthAndModificationTime() throws IOException {
        S3StorageProvider provider = new S3StorageProvider(S3Configuration.fromMap(Map.of("auth", "anonymous")), mockS3);

        StorageObject object = provider.newObject(PATH, FILE_SIZE, LAST_MODIFIED);

        assertEquals(FILE_SIZE, object.length());
        assertEquals(LAST_MODIFIED, object.lastModified());
        verify(mockS3, never()).headObject(any(HeadObjectRequest.class));
        verify(mockS3, never()).getObject(any(GetObjectRequest.class));
    }

    /**
     * length() is answered by a first-byte range GET, not a HEAD.
     */
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
