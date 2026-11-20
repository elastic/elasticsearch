/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RecordSuccessRequest;
import software.amazon.awssdk.retries.api.RecordSuccessResponse;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.MeteredInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
        TransientTypingInputStream typed = new TransientTypingInputStream(stream, PATH, 100_000);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(typed);

        assertTrue("close of a large leftover must abort the Abortable inner", abortCalled.get());
        assertFalse("close() must not drain a large leftover", closeCalled.get());
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
        TransientTypingInputStream typed = new TransientTypingInputStream(inner, PATH, 100_000);
        MeteredInputStream metered = new MeteredInputStream(typed, new StorageObjectMetricsCounters());

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(metered);

        assertTrue("Metered close of a large leftover must reach the inner Abortable", abortCalled.get());
        assertFalse("close() must not drain a large leftover", closeCalled.get());
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
        TransientTypingInputStream wrapped = new TransientTypingInputStream(inner, PATH, 100_000);

        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.abortStream(wrapped);

        assertTrue("abortStream on the wrapped stream must call abort(), not Apache close()", abortCalled.get());
        assertFalse("close() must not be called when abort() is available — close would drain", closeCalled.get());
    }

    public void testCloseCreditsDrainedRemainderAtOrBelow64KiB() throws IOException {
        int leftover = TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES;
        int read = 8;
        byte[] payload = new byte[read + leftover];
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        TransientTypingInputStream typed = new TransientTypingInputStream(
            new ByteArrayInputStream(payload),
            PATH,
            payload.length,
            leftoverBytes -> {
                if (leftoverBytes > 0) {
                    counters.publishDrainedBytes(leftoverBytes);
                }
            }
        );
        MeteredInputStream metered = new MeteredInputStream(typed, counters);
        assertEquals(read, metered.read(new byte[read]));
        metered.close();
        assertEquals(read + leftover, counters.snapshot().bytesRead());
        assertEquals("Apache close drains leftover; APM matches drained bytes", read + leftover, apmBytesReadTotal(registry));
    }

    public void testCloseAfterReadFaultDoesNotCreditLeftover() throws IOException {
        int leftover = 64;
        int delivered = 8;
        AtomicLong credited = new AtomicLong();
        AtomicBoolean abortCalled = new AtomicBoolean();
        AtomicBoolean closeCalled = new AtomicBoolean();
        InputStream inner = new AbortableInputStream(new FilterInputStream(new ByteArrayInputStream(new byte[delivered + leftover])) {
            int seen;

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (seen >= delivered) {
                    throw new IOException("reset");
                }
                int got = super.read(b, off, Math.min(len, delivered - seen));
                if (got > 0) {
                    seen += got;
                }
                return got;
            }
        }, abortCalled, closeCalled);
        TransientTypingInputStream typed = new TransientTypingInputStream(inner, PATH, delivered + leftover, credited::addAndGet);
        MeteredInputStream metered = new MeteredInputStream(typed, new StorageObjectMetricsCounters());
        assertEquals(delivered, metered.read(new byte[delivered]));
        expectThrows(ExternalUnavailableException.class, () -> metered.read(new byte[1]));
        metered.close();
        assertEquals("failed-stream drain must not credit leftover the resume path will fetch", 0L, credited.get());
        assertTrue("failed-stream close must abort; Apache drain of that socket can stall resume", abortCalled.get());
        assertFalse(closeCalled.get());
    }

    public void testCloseAbortsRemainderAbove64KiB() throws IOException {
        int leftover = TransientTypingInputStream.MAX_TRAILING_DRAIN_BYTES + 1;
        int read = 8;
        byte[] payload = new byte[read + leftover];
        AtomicBoolean abortCalled = new AtomicBoolean();
        AtomicBoolean closeCalled = new AtomicBoolean();
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        AbortableInputStream inner = new AbortableInputStream(new ByteArrayInputStream(payload), abortCalled, closeCalled);
        TransientTypingInputStream typed = new TransientTypingInputStream(inner, PATH, payload.length);
        MeteredInputStream metered = new MeteredInputStream(typed, counters);
        assertEquals(read, metered.read(new byte[read]));
        metered.close();
        assertEquals(read, counters.snapshot().bytesRead());
        assertEquals("abort skips leftover; APM is delivered-to-caller only", read, apmBytesReadTotal(registry));
        assertTrue(abortCalled.get());
        assertFalse(closeCalled.get());
    }

    /**
     * Two in-flight Apache GETs must both unblock when the object is closed. {@code close()}
     * unparks the waiting {@code getObject} threads (and aborts the Apache execute in production).
     */
    public void testCloseCancelsInFlightSyncGets() throws Exception {
        CountDownLatch started = new CountDownLatch(2);
        when(mockS3.getObject(any(GetObjectRequest.class))).thenAnswer(inv -> {
            started.countDown();
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(30));
            throw SdkClientException.create("getObject still parked after close");
        });
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        Thread t1 = new Thread(() -> {
            try {
                obj.newStream();
            } catch (Exception ignored) {}
        });
        Thread t2 = new Thread(() -> {
            try {
                obj.newStream();
            } catch (Exception ignored) {}
        });
        t1.start();
        t2.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        obj.close();
        t1.join(TimeUnit.SECONDS.toMillis(5));
        t2.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(t1.isAlive());
        assertFalse(t2.isAlive());
    }

    public void testObjectCloseAbortsLiveGetStream() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean();
        GetObjectResponse resp = GetObjectResponse.builder().contentLength(100_000L).build();
        AbortableInputStream inner = new AbortableInputStream(
            new ByteArrayInputStream("partial".getBytes(StandardCharsets.UTF_8)),
            abortCalled,
            new AtomicBoolean()
        );
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, software.amazon.awssdk.http.AbortableInputStream.create(inner, inner))
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        obj.newStream();
        obj.close();
        assertTrue("StorageObject.close() must abort a live Apache GET", abortCalled.get());
    }

    public void testObjectCloseAbortsInProgressApacheDrain() throws Exception {
        CountDownLatch draining = new CountDownLatch(1);
        AtomicBoolean abortCalled = new AtomicBoolean();
        AtomicReference<Thread> drainThread = new AtomicReference<>();
        class ParkingAbortable extends FilterInputStream implements Abortable {
            ParkingAbortable() {
                super(new ByteArrayInputStream(new byte[8]));
            }

            @Override
            public void abort() {
                abortCalled.set(true);
                Thread t = drainThread.get();
                if (t != null) {
                    LockSupport.unpark(t);
                }
            }

            @Override
            public void close() {
                drainThread.set(Thread.currentThread());
                draining.countDown();
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(30));
            }
        }
        ParkingAbortable inner = new ParkingAbortable();
        GetObjectResponse resp = GetObjectResponse.builder().contentLength(8L).build();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(resp, software.amazon.awssdk.http.AbortableInputStream.create(inner, inner))
        );
        S3StorageObject obj = new S3StorageObject(mockS3, BUCKET, KEY, PATH);
        InputStream stream = obj.newStream();
        Thread closer = new Thread(() -> {
            try {
                stream.close();
            } catch (IOException ignored) {
                // drain interrupted by abort
            }
        });
        closer.start();
        assertTrue(draining.await(5, TimeUnit.SECONDS));
        obj.close();
        closer.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(closer.isAlive());
        assertTrue("StorageObject.close() must abort an in-progress Apache drain", abortCalled.get());
    }

    /**
     * Blocking GET shares {@code s3-streaming-get} with every object. {@code refreshRetryToken}
     * takes tokens; only {@code recordSuccess} returns them. Without that call, ~100 retried GETs
     * open the circuit and IAM-lag 403s stop retrying.
     */
    @SuppressWarnings("unchecked")
    public void testBlockingGetRecordsRetrySuccessAfterRetry() throws IOException {
        S3AsyncClient async = mock(S3AsyncClient.class);
        GetObjectResponse resp = GetObjectResponse.builder().contentLength(4L).build();
        ResponseInputStream<GetObjectResponse> body = new ResponseInputStream<>(
            resp,
            software.amazon.awssdk.http.AbortableInputStream.create(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)))
        );
        AtomicInteger attempts = new AtomicInteger();
        when(mockS3.getObject(any(GetObjectRequest.class))).thenAnswer(inv -> {
            if (attempts.getAndIncrement() == 0) {
                throw SdkClientException.create("transient", new IOException("transient"));
            }
            return body;
        });
        AtomicInteger successes = new AtomicInteger();
        RetryStrategy delegate = AwsRetryStrategy.standardRetryStrategy()
            .toBuilder()
            .backoffStrategy(BackoffStrategy.retryImmediately())
            .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())
            .build();
        RetryStrategy counting = new RetryStrategy() {
            @Override
            public AcquireInitialTokenResponse acquireInitialToken(AcquireInitialTokenRequest request) {
                return delegate.acquireInitialToken(request);
            }

            @Override
            public RefreshRetryTokenResponse refreshRetryToken(RefreshRetryTokenRequest request) {
                return delegate.refreshRetryToken(request);
            }

            @Override
            public RecordSuccessResponse recordSuccess(RecordSuccessRequest request) {
                successes.incrementAndGet();
                return delegate.recordSuccess(request);
            }

            @Override
            public int maxAttempts() {
                return delegate.maxAttempts();
            }

            @Override
            public Builder<?, ?> toBuilder() {
                return delegate.toBuilder();
            }
        };
        S3StorageObject obj = new S3StorageObject(mockS3, async, counting, BUCKET, KEY, PATH);
        try (InputStream in = obj.newStream()) {
            assertEquals('d', in.read());
        }
        assertEquals(2, attempts.get());
        assertEquals(1, successes.get());
    }

    private static long apmBytesReadTotal(RecordingMeterRegistry registry) {
        return registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
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
