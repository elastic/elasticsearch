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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
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
     * {@code newStream} must return the Abortable {@link TransientTypingInputStream} so gzip/probe
     * {@code abortStream} does not fall through to a draining Apache close.
     */
    public void testNewStreamReturnsAbortableTransientTypingInputStream() throws IOException {
        byte[] body = "partial".getBytes(StandardCharsets.UTF_8);
        GetObjectResponse resp = GetObjectResponse.builder().contentLength((long) body.length).build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, software.amazon.awssdk.http.AbortableInputStream.create(new ByteArrayInputStream(body)))
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        InputStream stream = obj.newStream();
        assertThat(stream, instanceOf(TransientTypingInputStream.class));
        assertThat(stream, instanceOf(Abortable.class));
        stream.close();
    }

    public void testNewStreamRangeReturnsAbortableTransientTypingInputStream() throws IOException {
        byte[] body = new byte[16];
        GetObjectResponse resp = GetObjectResponse.builder().contentLength((long) body.length).build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, software.amazon.awssdk.http.AbortableInputStream.create(new ByteArrayInputStream(body)))
        );

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        InputStream stream = obj.newStream(0, body.length);
        assertThat(stream, instanceOf(TransientTypingInputStream.class));
        assertThat(stream, instanceOf(Abortable.class));
        stream.close();
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
