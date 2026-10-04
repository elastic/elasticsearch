/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.MeteredInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3StorageObject#abortStream(InputStream)} dispatch.
 * <p>
 * The slower-but-realistic counterpart is {@code S3CloseDrainProbeTests}, which measures real
 * drain-vs-abort cost against a public S3 object and is gated to run only when explicitly
 * enabled. These unit tests cover the in-process dispatch logic so a future refactor that
 * silently drops the {@code Abortable} cast (e.g. by swapping {@code ResponseInputStream} for
 * a non-{@code Abortable} wrapper) is caught in CI.
 */
public class S3StorageObjectAbortStreamTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/file.csv.gz";
    private static final StoragePath PATH = StoragePath.of("s3://" + BUCKET + "/" + KEY);

    private final S3Client mockS3 = mock(S3Client.class);

    /**
     * Regression guard: when the stream implements {@link Abortable} (as the real S3
     * {@code ResponseInputStream} does), {@code abortStream} must call {@code abort()} to
     * discard the underlying HTTP connection without draining. Falling back to
     * {@code close()} would trigger Apache HttpClient's content-length drain — the exact
     * bug this whole change exists to prevent.
     */
    public void testAbortStreamCallsAbortWhenAbortable() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean(false);
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        AbortableInputStream stream = new AbortableInputStream(
            new ByteArrayInputStream("partial".getBytes(StandardCharsets.UTF_8)),
            abortCalled,
            closeCalled
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(stream);

        assertTrue("abort() must be called on Abortable streams", abortCalled.get());
        assertFalse("close() must not be called when abort() is available — close would drain", closeCalled.get());
    }

    /**
     * Regression guard: for streams that do not implement {@link Abortable} (defensive path
     * for stubs, custom transformers, etc.) {@code abortStream} must fall back to
     * {@code close()} so the stream is still released.
     */
    public void testAbortStreamFallsBackToCloseWhenNotAbortable() throws IOException {
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        InputStream stream = new FilterInputStream(new ByteArrayInputStream(new byte[0])) {
            @Override
            public void close() throws IOException {
                closeCalled.set(true);
                super.close();
            }
        };

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(stream);

        assertTrue("close() must be invoked on non-Abortable streams as a fallback", closeCalled.get());
    }

    /**
     * MeteredInputStream is not AWS {@link Abortable}. abortStream must dispatch on Metered
     * first; {@code instanceof Abortable} would miss it and fall through to close(), which drains.
     */
    public void testAbortStreamCallsMeteredAbort() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean(false);
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        AbortableInputStream inner = new AbortableInputStream(
            new ByteArrayInputStream("partial".getBytes(StandardCharsets.UTF_8)),
            abortCalled,
            closeCalled
        );
        MeteredInputStream metered = new MeteredInputStream(inner, new StorageObjectMetricsCounters(), inner::abort);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(metered);

        assertTrue("Metered abort must reach the inner Abortable", abortCalled.get());
        assertFalse("close() must not drain after Metered abort", closeCalled.get());
    }

    public void testAbortStreamOnNewStreamUsesTypedAbort() throws IOException {
        GetObjectResponse resp = GetObjectResponse.builder().contentLength(7L).build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(
                resp,
                software.amazon.awssdk.http.AbortableInputStream.create(
                    new ByteArrayInputStream("partial".getBytes(StandardCharsets.UTF_8))
                )
            )
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        InputStream stream = obj.newStream();
        assertTrue("leaf newStream must return Metered so abortStream does not drain", stream instanceof MeteredInputStream);
        obj.abortStream(stream);
    }

    public void testAbortStreamOnTransientTypingInputStreamCallsAbortNotClose() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean(false);
        AtomicBoolean closeCalled = new AtomicBoolean(false);
        AbortableInputStream inner = new AbortableInputStream(
            new ByteArrayInputStream("partial".getBytes(StandardCharsets.UTF_8)),
            abortCalled,
            closeCalled
        );
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, 1000);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(wrapped);

        assertTrue("abortStream on the wrapped stream must call abort(), not Apache close()", abortCalled.get());
        assertFalse("close() must not be called when abort() is available — close would drain", closeCalled.get());
    }

    public void testCloseDrainCountsRemainderAtOrBelow64KiB() throws IOException {
        int leftover = TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES;
        int read = 8;
        byte[] payload = new byte[read + leftover];
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        TransientTypingInputStream typed = new TransientTypingInputStream(
            new ByteArrayInputStream(payload),
            PATH,
            payload.length,
            leftoverBytes -> counters.addBytes(leftoverBytes)
        );
        MeteredInputStream metered = new MeteredInputStream(typed, counters, typed::abort);
        assertEquals(read, metered.read(new byte[read]));
        metered.close();
        assertEquals(payload.length, counters.snapshot().bytesRead());
    }

    public void testCloseAbortsRemainderAbove64KiB() throws IOException {
        int leftover = TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 1;
        int read = 8;
        byte[] payload = new byte[read + leftover];
        AtomicBoolean abortCalled = new AtomicBoolean();
        AtomicBoolean closeCalled = new AtomicBoolean();
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        AbortableInputStream inner = new AbortableInputStream(new ByteArrayInputStream(payload), abortCalled, closeCalled);
        TransientTypingInputStream typed = new TransientTypingInputStream(
            inner,
            PATH,
            payload.length,
            leftoverBytes -> counters.addBytes(leftoverBytes)
        );
        MeteredInputStream metered = new MeteredInputStream(typed, counters, typed::abort);
        assertEquals(read, metered.read(new byte[read]));
        metered.close();
        assertEquals(read, counters.snapshot().bytesRead());
        assertTrue(abortCalled.get());
        assertFalse(closeCalled.get());
    }

    /**
     * An {@link InputStream} that also implements {@link Abortable}, mimicking the shape of
     * the real S3 {@code ResponseInputStream}. The {@code abortCalled} / {@code closeCalled}
     * flags let the test distinguish which dispatch path the wrapper took.
     */
    private static final class AbortableInputStream extends FilterInputStream implements Abortable {
        private final AtomicBoolean abortCalled;
        private final AtomicBoolean closeCalled;

        AbortableInputStream(InputStream in, AtomicBoolean abortCalled, AtomicBoolean closeCalled) {
            super(in);
            this.abortCalled = abortCalled;
            this.closeCalled = closeCalled;
        }

        @Override
        public void abort() {
            abortCalled.set(true);
        }

        @Override
        public void close() throws IOException {
            closeCalled.set(true);
            super.close();
        }
    }
}
