/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryableStorageObjectTests extends ESTestCase {

    /**
     * Test fixture instead of {@code mock(StorageObject.class)} per AGENTS.md "real classes over mocks":
     * carries a configurable {@link StorageObjectMetrics} snapshot and a single-shot {@code newStream()}
     * that fails {@code failuresBeforeSuccess} times with the configured exception then returns the
     * configured payload. Other {@link StorageObject} methods throw — they're not exercised by the
     * metrics tests below and a real failure beats a silent mocked default.
     */
    private static final class FakeStorageObject implements StorageObject {
        private final StoragePath path;
        private final StorageObjectMetrics metrics;
        private final IOException failure;
        private final byte[] successPayload;
        private final int failuresBeforeSuccess;
        private int callsObserved = 0;

        FakeStorageObject(
            StoragePath path,
            StorageObjectMetrics metrics,
            IOException failure,
            byte[] successPayload,
            int failuresBeforeSuccess
        ) {
            this.path = path;
            this.metrics = metrics;
            this.failure = failure;
            this.successPayload = successPayload;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public InputStream newStream() throws IOException {
            int call = callsObserved++;
            if (call < failuresBeforeSuccess) {
                throw failure;
            }
            return new ByteArrayInputStream(successPayload);
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant lastModified() {
            // SPI contract: "or null if not available" — null is a real value, not unsupported.
            return null;
        }

        @Override
        public boolean exists() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return metrics;
        }
    }

    public void testAsyncRetrySchedulesContinuationInsteadOfSleeping() {
        // A transient async failure must reschedule the retry through the RetryScheduler (an off-timer
        // continuation), NOT park a thread on Thread.sleep for the backoff. The fake scheduler records the
        // requested delay and runs the continuation inline, so no real waiting happens in the test.
        StoragePath path = StoragePath.of("s3://bucket/key");
        AtomicInteger attempts = new AtomicInteger();
        StorageObject flaky = new StorageObject() {
            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Instant lastModified() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean exists() {
                throw new UnsupportedOperationException();
            }

            @Override
            public StoragePath path() {
                return path;
            }

            @Override
            public int readBytes(long position, ByteBuffer target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void readBytesAsync(
                long position,
                long len,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                if (attempts.getAndIncrement() == 0) {
                    listener.onFailure(new ExternalUnavailableException("transient async read failure", (Throwable) null));
                } else {
                    listener.onResponse(new DirectReadBuffer(ByteBuffer.allocate(4), () -> {}));
                }
            }

            @Override
            public StorageObjectMetrics metrics() {
                return new StorageObjectMetrics(0, 0, 0, 0);
            }
        };

        List<Long> scheduledDelays = new ArrayList<>();
        RetryScheduler capturingScheduler = (command, delayMillis, executor) -> {
            scheduledDelays.add(delayMillis);
            command.run();
        };

        RetryableStorageObject obj = new RetryableStorageObject(flaky, new RetryPolicy(3, 5, 50), capturingScheduler);

        AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        obj.readBytesAsync(
            0,
            4,
            len -> new DirectReadBuffer(ByteBuffer.allocate(len), () -> {}),
            Runnable::run,
            ActionListener.wrap(result::set, failure::set)
        );

        assertNull("async read should recover via the scheduled retry, not fail", failure.get());
        assertNotNull("async read should deliver a buffer after the scheduled retry", result.get());
        assertEquals("the single transient failure must reschedule exactly one retry continuation", 1, scheduledDelays.size());
        assertEquals("the read must succeed on the second attempt", 2, attempts.get());
    }

    public void testMetricsForwardsDelegateCountersWhenNoRetries() {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        StorageObjectMetrics snapshot = new StorageObjectMetrics(13, 4321, 16384, 4);
        FakeStorageObject delegate = new FakeStorageObject(StoragePath.of("s3://bucket/key"), snapshot, null, new byte[0], 0);

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        // No retries observed at this decorator boundary, so the merged snapshot equals the delegate.
        assertEquals(snapshot, obj.metrics());
    }

    public void testMetricsAddsRetryCountObservedByDecorator() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        // Underlying provider counts a request and bytes; SDK-internal retries (would-be retryCount)
        // are not yet wired at the provider layer, so retryCount on the delegate is zero.
        StorageObjectMetrics delegateMetrics = new StorageObjectMetrics(1, 100, 4096, 0);
        // Two SocketTimeoutException failures classify as transient (per RetryPolicy.isTransientSingleCause),
        // triggering two retries before the third newStream() call returns a real ByteArrayInputStream.
        FakeStorageObject delegate = new FakeStorageObject(
            StoragePath.of("s3://bucket/key"),
            delegateMetrics,
            new SocketTimeoutException("read timed out"),
            new byte[] { 1, 2, 3 },
            2
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        try (InputStream returned = obj.newStream()) {
            assertEquals(1, returned.read());
            assertEquals(2, returned.read());
            assertEquals(3, returned.read());
            assertEquals(-1, returned.read());
        }

        StorageObjectMetrics merged = obj.metrics();
        // Delegate's request/byte counters pass through; retry counter accumulates at the decorator.
        assertEquals(1L, merged.requestCount());
        assertEquals(100L, merged.requestNanos());
        assertEquals(4096L, merged.bytesRead());
        assertEquals(2L, merged.retryCount());
    }

    /**
     * Wiring test for the retry->node-telemetry bridge on the RECOVERY path: two transient failures that then
     * succeed must move the registry {@code storage.retries.total} counter (tagged with the scheme), not just the
     * profile-snapshot {@code retryCount()}. Complements {@link #testMetricsAddsRetryCountObservedByDecorator}
     * (which asserts only the snapshot). Uses a real registry-backed holder attached to a real
     * {@link RetryableStorageObject} so the production {@code addRetry()} -> {@code recordRetry()} -> registry path
     * is exercised end to end on a successful read.
     */
    public void testRetriesBridgeToRegistryCounterOnRecovery() throws IOException {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        // Two SocketTimeoutException failures classify as transient, triggering two retries before the third
        // newStream() call returns a real ByteArrayInputStream. Delegate carries no SDK-internal retries.
        FakeStorageObject delegate = new FakeStorageObject(
            StoragePath.of("s3://bucket/key"),
            new StorageObjectMetrics(1, 100, 4096, 0),
            new SocketTimeoutException("read timed out"),
            new byte[] { 1, 2, 3 },
            2
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        obj.attachMetrics(metrics, "s3");

        try (InputStream returned = obj.newStream()) {
            assertEquals(1, returned.read());
            assertEquals(2, returned.read());
            assertEquals(3, returned.read());
            assertEquals(-1, returned.read());
        }

        // Profile snapshot accumulates the two decorator-observed retries...
        assertEquals("profile snapshot must observe both retries", 2L, obj.metrics().retryCount());

        // ...and the same two retries reach the registry counter, tagged with the scheme.
        List<Measurement> retries = measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL);
        assertThat("each retry publishes one registry observation", retries, hasSize(2));
        long total = retries.stream().mapToLong(Measurement::getLong).sum();
        assertThat("registry storage.retries.total must move by the retry count", total, equalTo(2L));
        for (Measurement m : retries) {
            assertThat(m.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));
        }
    }

    /**
     * Wiring test for the retry->node-telemetry bridge: a terminal give-up on the sync open path must publish a
     * storage error AND the cumulative backoff as a read stall to the attached {@link ExternalSourceMetrics},
     * tagged with the storage scheme. Uses a real registry-backed metrics holder (no mock) so the production
     * {@code attachMetrics} -> {@code recordTerminalFailure} -> counters -> registry path is exercised end to end.
     */
    public void testTerminalGiveUpBridgesErrorAndStallToRegistry() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        // A transient (non-throttle) fault that never clears: the 1-retry budget is exhausted and the open gives up.
        AlwaysFailingStorageObject delegate = new AlwaysFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            new SocketTimeoutException("read timed out")
        );
        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(1, 5, 10));
        obj.attachMetrics(metrics, "s3");

        expectThrows(IOException.class, obj::newStream);

        Measurement error = single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL);
        assertThat(error.getLong(), equalTo(1L));
        assertThat(error.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));
        // One retry backoff was spent before giving up, so a read-stall observation is recorded (>0).
        assertThat(measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION), hasSize(1));
        // A non-throttle fault must not touch the throttled counter.
        assertThat(measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL), hasSize(0));
    }

    /**
     * Wiring test: a terminal give-up whose fault is a provider throttle must additionally bump the throttled
     * counter (on top of the generic error counter), via the same bridge.
     */
    public void testThrottlingGiveUpBridgesThrottledToRegistry() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        AlwaysFailingStorageObject delegate = new AlwaysFailingStorageObject(
            StoragePath.of("gcs://bucket/key"),
            new ExternalUnavailableException(true, "throttled")
        );
        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(1, 1, 10));
        obj.attachMetrics(metrics, "gcs");

        expectThrows(ExternalUnavailableException.class, obj::newStream);

        Measurement throttled = single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL);
        assertThat(throttled.getLong(), equalTo(1L));
        assertThat(throttled.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("gcs"));
        // The generic error counter is always bumped on a terminal give-up, throttle or not.
        assertThat(single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL).getLong(), equalTo(1L));
    }

    /**
     * Wiring test for the retries-disabled config ({@code maxRetries == 0 && throttleMaxRetries == 0}): the
     * fast path still fires the terminal give-up, so a fatal open is counted as a storage error. There was no
     * backoff, so no read stall is recorded (the histogram is not seeded with a zero).
     */
    public void testRetriesDisabledStillCountsTerminalError() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        AlwaysFailingStorageObject delegate = new AlwaysFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            new SocketTimeoutException("read timed out")
        );
        RetryableStorageObject obj = new RetryableStorageObject(delegate, RetryPolicy.NONE);
        obj.attachMetrics(metrics, "s3");

        expectThrows(IOException.class, obj::newStream);

        assertThat(single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL).getLong(), equalTo(1L));
        assertThat(measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION), hasSize(0));
    }

    /**
     * Regression for the item #3 sync/async asymmetry. The sync driver ({@code RetryPolicy.execute}) only records a
     * terminal storage error inside {@code catch (IOException | ExternalUnavailableException)}, but the async driver
     * reaches {@code recordTerminalFailure} for ANY terminal fault. A storage error must mean a storage-classified
     * fault, so a NON-storage terminal fault on the async path (here a {@link TaskCancelledException}; a
     * {@code CircuitBreakingException} from a native-async provider takes the same path) must NOT bump
     * {@code storage.errors} or {@code storage.throttled} — otherwise a breaker trip would double-surface as both a
     * storage error and {@code breaker.tripped}. The listener is still completed with the fault (best-effort
     * telemetry never strands it).
     */
    public void testAsyncNonStorageTerminalFaultDoesNotCountStorageError() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        StoragePath path = StoragePath.of("s3://bucket/key");
        // A native-async delegate whose read fails with a non-storage, non-transient fault, so the async driver
        // gives up on the first attempt and records the terminal failure.
        StorageObject delegate = new StorageObject() {
            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Instant lastModified() {
                return null;
            }

            @Override
            public boolean exists() {
                throw new UnsupportedOperationException();
            }

            @Override
            public StoragePath path() {
                return path;
            }

            @Override
            public int readBytes(long position, ByteBuffer target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void readBytesAsync(
                long position,
                long len,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                listener.onFailure(new TaskCancelledException("cancelled"));
            }

            @Override
            public StorageObjectMetrics metrics() {
                return new StorageObjectMetrics(0, 0, 0, 0);
            }
        };

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 5, 50));
        obj.attachMetrics(metrics, "s3");

        AtomicReference<Exception> failure = new AtomicReference<>();
        obj.readBytesAsync(
            0,
            4,
            len -> new DirectReadBuffer(ByteBuffer.allocate(len), () -> {}),
            Runnable::run,
            ActionListener.wrap(r -> fail("the read must not succeed"), failure::set)
        );

        assertThat("the listener must still receive the terminal fault", failure.get(), instanceOf(TaskCancelledException.class));
        assertThat(
            "a non-storage terminal fault must not count as a storage error",
            measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL),
            hasSize(0)
        );
        assertThat(
            "a non-storage terminal fault must not count as a throttle",
            measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL),
            hasSize(0)
        );
    }

    /**
     * B1 scope-invariant regression: a metadata-op retry must NOT move the read-scoped registry
     * {@code storage.retries.total}. Metadata ops ({@code length}/{@code lastModified}/{@code exists}) never bump the
     * read-scoped {@code storage.requests.total}, so counting their retries (or errors / read-stall) against the
     * registry would leak {@code storage.retries.total} past {@code storage.requests.total} — the exact scope
     * violation this fix restores ({@code retries}/{@code errors}/{@code read_stall} &le; {@code requests} on retryable
     * providers). The metadata retry MUST still surface in the per-query profile snapshot (pre-existing behaviour). For
     * contrast, a read-path ({@code newStream()}) retry DOES move {@code storage.retries.total}. Uses real
     * {@link RecordingMeterRegistry}-backed holders (no mocks) so the production {@code attachMetrics} -> retry ->
     * registry path is exercised end to end.
     */
    public void testMetadataOpRetryIsNotReadScopedButReadPathRetryIs() throws IOException {
        // --- Metadata-op path: length() throws one transient fault, then succeeds. ---
        RecordingMeterRegistry metadataRegistry = new RecordingMeterRegistry();
        ExternalSourceMetrics metadataMetrics = new ExternalSourceMetrics(metadataRegistry);
        MetadataFailingStorageObject metadataDelegate = new MetadataFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            new SocketTimeoutException("read timed out"),
            1,
            4096L
        );
        RetryableStorageObject metadataObj = new RetryableStorageObject(metadataDelegate, new RetryPolicy(3, 1, 10));
        metadataObj.attachMetrics(metadataMetrics, "s3");

        assertEquals("length() recovers after the single transient fault", 4096L, metadataObj.length());

        // The metadata-op retry must NOT reach the read-scoped registry counter.
        assertThat(
            "a metadata-op retry must not move the read-scoped storage.retries.total",
            measurements(metadataRegistry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL),
            hasSize(0)
        );
        // Nor may its (recovered) retry leak an error or a read-stall to the registry.
        assertThat(
            "a recovered metadata-op retry must not record a storage error",
            measurements(metadataRegistry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL),
            hasSize(0)
        );
        assertThat(
            "a metadata-op retry must not record a read-stall on the read-scoped histogram",
            measurements(metadataRegistry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION),
            hasSize(0)
        );
        // ...but the retry is still visible in the per-query profile snapshot (pre-existing profile behaviour).
        assertEquals("the metadata retry is still visible in the profile snapshot", 1L, metadataObj.metrics().retryCount());

        // --- Read-path contrast: newStream() throws one transient fault, then succeeds; this DOES move the registry. ---
        RecordingMeterRegistry readRegistry = new RecordingMeterRegistry();
        ExternalSourceMetrics readMetrics = new ExternalSourceMetrics(readRegistry);
        FakeStorageObject readDelegate = new FakeStorageObject(
            StoragePath.of("s3://bucket/key"),
            new StorageObjectMetrics(1, 100, 4096, 0),
            new SocketTimeoutException("read timed out"),
            new byte[] { 1, 2, 3 },
            1
        );
        RetryableStorageObject readObj = new RetryableStorageObject(readDelegate, new RetryPolicy(3, 1, 10));
        readObj.attachMetrics(readMetrics, "s3");
        try (InputStream in = readObj.newStream()) {
            in.readAllBytes();
        }

        List<Measurement> readRetries = measurements(
            readRegistry,
            InstrumentType.LONG_COUNTER,
            ExternalSourceMetrics.STORAGE_RETRIES_TOTAL
        );
        assertThat("a read-path retry MUST move the read-scoped storage.retries.total", readRetries, hasSize(1));
        assertThat("the read-path retry counts exactly once", readRetries.get(0).getLong(), equalTo(1L));
    }

    /**
     * B1 scope-invariant tripwire, terminal-give-up case: a metadata op ({@code length()}) whose transient fault
     * NEVER clears exhausts its retry budget and gives up terminally, yet must publish NONE of the read-scoped
     * registry metrics — not {@code storage.retries.total}, not {@code storage.errors.total}, not
     * {@code storage.read_stall.duration}. It stays off the registry only because {@code length}/{@code lastModified}/
     * {@code exists} pass {@code RetryTelemetry.NONE} (so the give-up records no error / read-stall) and route their
     * retries through {@code addRetryProfileOnly} (so no {@code storage.retries.total}). This is the exact tripwire that
     * catches a future edit swapping those ops' {@code RetryTelemetry.NONE} back to {@code storageTelemetry}: that swap
     * would fire {@code recordTerminalFailure} on the give-up and move {@code storage.errors.total} +
     * {@code storage.read_stall.duration}, reddening the assertions below.
     * <p>
     * Direct invariant assertion: after the give-up, {@code storage.retries.total == 0} AND
     * {@code storage.requests.total == 0} — a metadata op never bumps requests, so the {@code retries &le; requests}
     * scope invariant holds as {@code 0 &le; 0}. The retries still surface in the per-query profile snapshot
     * (pre-existing behaviour), which is where a metadata retry is legitimately visible. Uses a real
     * {@link RecordingMeterRegistry}-backed holder (no mocks).
     */
    public void testMetadataOpGiveUpStaysOffRegistry() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        // length() throws a transient fault on EVERY attempt, so the small (1-retry) budget is exhausted and the
        // metadata op gives up terminally. Integer.MAX_VALUE failures-before-success == never recovers.
        MetadataFailingStorageObject metadataDelegate = new MetadataFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            new SocketTimeoutException("read timed out"),
            Integer.MAX_VALUE,
            4096L
        );
        RetryableStorageObject metadataObj = new RetryableStorageObject(metadataDelegate, new RetryPolicy(1, 1, 10));
        metadataObj.attachMetrics(metrics, "s3");

        // One retry is spent (initial attempt + 1 retry) and then it gives up, surfacing the transient fault.
        expectThrows(IOException.class, metadataObj::length);

        // NONE of the read-scoped registry metrics may move for a metadata-op terminal give-up.
        List<Measurement> retries = measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL);
        List<Measurement> requests = measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL);
        assertThat("a metadata-op give-up must not move storage.retries.total", retries, hasSize(0));
        assertThat("a metadata op never bumps storage.requests.total", requests, hasSize(0));
        assertThat(
            "a metadata-op give-up must not record a storage error (RetryTelemetry.NONE swallows it)",
            measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL),
            hasSize(0)
        );
        assertThat(
            "a metadata-op give-up must not record a read-stall on the read-scoped histogram",
            measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION),
            hasSize(0)
        );

        // Direct invariant: retries (0) <= requests (0) holds, computed from the registry totals.
        long retriesTotal = retries.stream().mapToLong(Measurement::getLong).sum();
        long requestsTotal = requests.stream().mapToLong(Measurement::getLong).sum();
        assertEquals("registry storage.retries.total is 0 after a metadata give-up", 0L, retriesTotal);
        assertEquals("registry storage.requests.total is 0 for a metadata op", 0L, requestsTotal);
        assertThat("the retries <= requests scope invariant holds as 0 <= 0", retriesTotal, lessThanOrEqualTo(requestsTotal));

        // ...but the retry IS still visible in the per-query profile snapshot (pre-existing profile behaviour).
        assertEquals("the metadata retry is still visible in the profile snapshot", 1L, metadataObj.metrics().retryCount());
    }

    private static List<Measurement> measurements(RecordingMeterRegistry registry, InstrumentType type, String name) {
        return registry.getRecorder().getMeasurements(type, name);
    }

    private static Measurement single(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> found = measurements(registry, type, name);
        assertThat("expected exactly one measurement for [" + name + "]", found, hasSize(1));
        return found.get(0);
    }

    /**
     * Regression guard: {@code abortStream} must forward directly to the delegate, not fall
     * through to the SPI default (which is a draining {@code stream.close()} on providers like
     * S3). The original bug was a missing override on a decorator silently swallowing the abort.
     */
    public void testAbortStreamDelegates() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        verify(delegate).abortStream(stream);
    }

    /**
     * Regression guard: abort is a best-effort connection-discard, not a retryable I/O. If the
     * delegate's {@code abortStream} throws, the exception must propagate unchanged — wrapping
     * abort in retry logic would defeat the bounded-latency contract the abort path provides.
     */
    public void testAbortStreamDoesNotRetryOnFailure() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        doThrow(new IOException("abort failed")).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(5, 1, 10));
        IOException thrown = expectThrows(IOException.class, () -> obj.abortStream(stream));
        assertEquals("abort failed", thrown.getMessage());

        verify(delegate, times(1)).abortStream(stream);
    }

    /**
     * Regression guard: the wrapper must not unwrap or alter the stream instance — the SPI
     * contract requires the delegate to receive the exact instance it returned from
     * {@code newStream()}, otherwise provider-specific abort dispatch (e.g. the S3
     * {@code Abortable} cast) silently falls back to a draining close.
     */
    public void testAbortStreamForwardsSameInstance() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        InputStream[] captured = new InputStream[1];
        doAnswer(inv -> {
            captured[0] = inv.getArgument(0);
            return null;
        }).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        assertSame(stream, captured[0]);
    }

    /**
     * A transient transport fault <em>during</em> a range read must re-open the remaining byte range and
     * resume, delivering every byte exactly once (object content is immutable). The re-open requests
     * {@code [position + delivered, end]}, so nothing is duplicated or skipped.
     */
    public void testRangeReadResumesByteExactAfterMidReadFault() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        // First range-read delivers 400 bytes then drops; the re-open from byte 400 delivers the rest.
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            400,
            new ExternalUnavailableException("mid-read drop", new IOException("premature end of body"))
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        byte[] read;
        try (InputStream in = obj.newStream(0, payload.length)) {
            read = in.readAllBytes();
        }

        assertArrayEquals("every byte delivered exactly once, byte-exact resume", payload, read);
        assertEquals("the range was re-opened exactly once", 2, delegate.openCount());
        assertEquals("the resume was counted as a retry", 1L, obj.metrics().retryCount());
    }

    /**
     * The whole-object read ({@code newStream()}) routes through the open-ended path ({@code newStream(0)}) and
     * gets the same byte-exact resume as a ranged read, in unknown-end mode — this is what covers single-file
     * text, compressed text and the ORC seed stream. The fault here is a raw {@link SocketException} (not a
     * pre-typed marker): it must be classified transient through the real predicate and resumed from the exact
     * offset, with no byte duplicated or skipped.
     */
    public void testWholeObjectReadResumesByteExactAfterMidReadFault() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        // Whole-object open delivers 350 bytes then the connection resets; the open-ended re-open from byte 350
        // delivers the rest.
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/whole.ndjson"),
            payload,
            350,
            new SocketException("Connection reset")
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        byte[] read;
        try (InputStream in = obj.newStream()) {
            read = in.readAllBytes();
        }

        assertArrayEquals("whole-object read recovers the exact payload across a mid-read reset", payload, read);
        assertEquals("the object was re-opened exactly once", 2, delegate.openCount());
        assertEquals("the resume was counted as a retry", 1L, obj.metrics().retryCount());
    }

    /**
     * Stacked mid-read faults at the unit layer: several drops in one read, each after delivering a chunk of
     * progress. The per-episode budget (maxRetries=3) resets on every byte of progress, so even 5 drops — well
     * over the budget — all recover byte-exact. If this passes while the E2E 3-fault case fails, the E2E failure
     * is in the S3/fixture/SDK-pin interaction, not the resume core.
     */
    public void testManyProgressingMidReadFaultsAllRecover() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        ChunkedFaultingStorageObject delegate = new ChunkedFaultingStorageObject(
            StoragePath.of("s3://bucket/k"),
            payload,
            150,
            5,
            new SocketException("Connection reset")
        );
        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        byte[] read;
        try (InputStream in = obj.newStream(0, payload.length)) {
            read = in.readAllBytes();
        }
        assertArrayEquals("byte-exact across 5 progressing mid-read faults", payload, read);
    }

    /**
     * A non-transient error mid-read (e.g. a permission error surfacing on the stream) must propagate
     * unchanged — no re-open, no retry.
     */
    public void testRangeReadPropagatesNonTransientMidReadError() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        byte[] payload = new byte[200];
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            40,
            new IOException("Access Denied")
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        try (InputStream in = obj.newStream(0, payload.length)) {
            IOException thrown = expectThrows(IOException.class, in::readAllBytes);
            assertEquals("Access Denied", thrown.getMessage());
        }
        assertEquals("a non-transient error must not trigger a re-open", 1, delegate.openCount());
    }

    /**
     * A stream stuck at the same offset — every re-open fails before delivering a byte — must fail cleanly
     * once the retry budget is exhausted, surfacing the transient fault rather than hanging or looping.
     */
    public void testRangeReadFailsAfterRetryBudgetExhausted() throws IOException {
        RetryPolicy policy = new RetryPolicy(2, 1, 10);
        byte[] payload = new byte[500];
        // Every open fails immediately with zero progress — the fault never clears.
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            0,
            new ExternalUnavailableException("persistent drop", new IOException("premature end of body")),
            true
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        try (InputStream in = obj.newStream(0, payload.length)) {
            // The injected transient fault is an ExternalUnavailableException; on budget exhaustion the resume
            // loop rethrows the ORIGINAL fault preserving its type, so the give-up surfaces as that EUE (not an
            // IOException). A non-transient give-up (see testRangeReadPropagatesNonTransientMidReadError) still
            // surfaces as the original IOException.
            expectThrows(ExternalUnavailableException.class, in::readAllBytes);
        }
        // Initial open + 2 retries (the budget), then it gives up.
        assertEquals("re-opened up to the retry budget then gave up", 3, delegate.openCount());
    }

    /**
     * A stuck stream under a long throttle backoff would otherwise sleep this thread for the full budget;
     * under an active cancellation scope the mid-read resume must abort promptly instead of sleeping, and
     * must not re-open the range.
     */
    public void testMidReadResumeAbortsBackoffWhenCancelled() throws IOException {
        // Long throttle delays so a non-aborting resume would sleep for a long time.
        RetryPolicy policy = new RetryPolicy(0, 0, 0, 10, 30_000, 30_000, RetryPolicy.NO_BUDGET, null);
        byte[] payload = new byte[500];
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            0,
            new ExternalUnavailableException(true, "throttled"),
            true
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        try (InputStream in = obj.newStream(0, payload.length)) {
            expectThrows(TaskCancelledException.class, () -> StorageRetryCancellation.runWithCancellation(() -> true, in::readAllBytes));
        }
        assertEquals("cancellation aborted the resume before re-opening the range", 1, delegate.openCount());
    }

    /**
     * Like {@link #testMidReadResumeAbortsBackoffWhenCancelled} but the signal flips true only <em>after</em>
     * the backoff sleep has started (not cancelled at the pre-sleep poll), so it exercises the in-sleep
     * cancellation polling rather than the immediate pre-sleep check.
     */
    public void testMidReadResumeAbortsBackoffWhenCancelledDuringSleep() throws IOException {
        RetryPolicy policy = new RetryPolicy(0, 0, 0, 10, 30_000, 30_000, RetryPolicy.NO_BUDGET, null);
        byte[] payload = new byte[500];
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            0,
            new ExternalUnavailableException(true, "throttled"),
            true
        );

        // false at the sleep start, true on the next in-sleep poll.
        AtomicInteger polls = new AtomicInteger();
        BooleanSupplier cancel = () -> polls.incrementAndGet() > 1;

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        long startNanos = System.nanoTime();
        try (InputStream in = obj.newStream(0, payload.length)) {
            expectThrows(TaskCancelledException.class, () -> StorageRetryCancellation.runWithCancellation(cancel, in::readAllBytes));
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        assertEquals("cancellation aborted the resume before re-opening the range", 1, delegate.openCount());
        assertThat("a cancel during the backoff must abort, not wait out the full delay", elapsedMs, lessThan(5_000L));
    }

    /**
     * A stream that drops repeatedly but makes progress each time is <em>not</em> stuck: the budget resets
     * on every byte delivered, so it re-opens as many times as needed and completes byte-exact. This is the
     * realistic "flaky connection over a large object" case.
     */
    public void testRangeReadCompletesWhenEachReopenMakesProgress() throws IOException {
        RetryPolicy policy = new RetryPolicy(2, 1, 10);
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        // Every open delivers 150 bytes then drops — but each makes progress, so it never exhausts.
        MidReadFailingStorageObject delegate = new MidReadFailingStorageObject(
            StoragePath.of("s3://bucket/key"),
            payload,
            150,
            new ExternalUnavailableException("flaky drop", new IOException("premature end of body")),
            true
        );

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        byte[] read;
        try (InputStream in = obj.newStream(0, payload.length)) {
            read = in.readAllBytes();
        }
        assertArrayEquals("progress-making drops still complete byte-exact", payload, read);
    }

    /**
     * closeQuietly() must swallow an <em>unchecked</em> close failure (e.g. the AWS SDK's {@code AbortedException})
     * on the discarded stream, so a noisy close never masks the resume. The first stream delivers some bytes,
     * drops with a transient fault, and throws {@link RuntimeException} from {@code close()}; the resume must still
     * re-open and complete byte-exact.
     */
    public void testResumeProceedsWhenDiscardedStreamCloseThrowsUnchecked() throws IOException {
        RetryPolicy policy = new RetryPolicy(3, 1, 10);
        byte[] payload = new byte[300];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        StoragePath path = StoragePath.of("s3://bucket/key");
        AtomicInteger opens = new AtomicInteger();
        StorageObject delegate = new StorageObject() {
            @Override
            public InputStream newStream(long position, long length) {
                int pos = Math.toIntExact(position);
                int len = length == READ_TO_END ? payload.length - pos : Math.toIntExact(Math.min(length, payload.length - pos));
                byte[] slice = Arrays.copyOfRange(payload, pos, pos + len);
                if (opens.getAndIncrement() == 0) {
                    // First open: deliver 100 bytes, then drop transient — and throw unchecked on close().
                    return new InputStream() {
                        private int p = 0;

                        @Override
                        public int read() throws IOException {
                            if (p >= 100) {
                                throw new ExternalUnavailableException("transient mid-read drop", new IOException("connection reset"));
                            }
                            return slice[p++] & 0xFF;
                        }

                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            if (p >= 100) {
                                throw new ExternalUnavailableException("transient mid-read drop", new IOException("connection reset"));
                            }
                            int n = Math.min(len, 100 - p);
                            System.arraycopy(slice, p, b, off, n);
                            p += n;
                            return n;
                        }

                        @Override
                        public void close() {
                            throw new RuntimeException("simulated AWS AbortedException on close");
                        }
                    };
                }
                return new ByteArrayInputStream(slice);
            }

            @Override
            public InputStream newStream() {
                return newStream(0, payload.length);
            }

            @Override
            public long length() {
                return payload.length;
            }

            @Override
            public Instant lastModified() {
                return null;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }

            @Override
            public int readBytes(long position, ByteBuffer target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public StorageObjectMetrics metrics() {
                return new StorageObjectMetrics(opens.get(), 0, 0, 0);
            }
        };

        RetryableStorageObject obj = new RetryableStorageObject(delegate, policy);
        byte[] read;
        try (InputStream in = obj.newStream(0, payload.length)) {
            read = in.readAllBytes();
        }
        assertArrayEquals("resume completes byte-exact despite an unchecked close on the discarded stream", payload, read);
        assertEquals("the resume re-opened exactly once after the unchecked-close discard", 2, opens.get());
    }

    /**
     * Test fixture for the terminal-give-up telemetry tests: every {@code newStream} open throws the configured
     * fault, so the retry budget is always exhausted and the operation gives up. The fault is either a checked
     * transport {@link IOException} or the unchecked {@link ExternalUnavailableException} a provider raises; it is
     * rethrown preserving its concrete type so the retry layer classifies it exactly as in production.
     */
    private static final class AlwaysFailingStorageObject implements StorageObject {
        private final StoragePath path;
        private final Exception failure;

        AlwaysFailingStorageObject(StoragePath path, Exception failure) {
            this.path = path;
            this.failure = failure;
        }

        private InputStream fail() throws IOException {
            if (failure instanceof IOException io) {
                throw io;
            }
            throw (RuntimeException) failure;
        }

        @Override
        public InputStream newStream() throws IOException {
            return fail();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            return fail();
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant lastModified() {
            return null;
        }

        @Override
        public boolean exists() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return new StorageObjectMetrics(0, 0, 0, 0);
        }
    }

    /**
     * Test fixture for the B1 metadata-op scope test: {@code length()} and {@code exists()} throw the configured
     * transient fault {@code failuresBeforeSuccess} times, then succeed ({@code length()} returns {@code lengthValue},
     * {@code exists()} returns {@code true}). The read paths are unsupported — the test drives only metadata ops, and a
     * real fault beats a silently-mocked default.
     */
    private static final class MetadataFailingStorageObject implements StorageObject {
        private final StoragePath path;
        private final IOException failure;
        private final int failuresBeforeSuccess;
        private final long lengthValue;
        private int lengthCalls = 0;
        private int existsCalls = 0;

        MetadataFailingStorageObject(StoragePath path, IOException failure, int failuresBeforeSuccess, long lengthValue) {
            this.path = path;
            this.failure = failure;
            this.failuresBeforeSuccess = failuresBeforeSuccess;
            this.lengthValue = lengthValue;
        }

        @Override
        public long length() throws IOException {
            if (lengthCalls++ < failuresBeforeSuccess) {
                throw failure;
            }
            return lengthValue;
        }

        @Override
        public boolean exists() throws IOException {
            if (existsCalls++ < failuresBeforeSuccess) {
                throw failure;
            }
            return true;
        }

        @Override
        public Instant lastModified() {
            return null;
        }

        @Override
        public InputStream newStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return new StorageObjectMetrics(0, 0, 0, 0);
        }
    }

    /**
     * Test fixture for the resuming-stream tests: a range read returns {@code [position, position+length)}
     * of the payload, but the first open (or every open, if {@code alwaysFail}) delivers only
     * {@code failAfterBytes} bytes before throwing the configured fault.
     */
    private static final class MidReadFailingStorageObject implements StorageObject {
        private final StoragePath path;
        private final byte[] payload;
        private final int failAfterBytes;
        // The injected mid-read fault is either a checked transport IOException (e.g. SocketException) or the
        // unchecked ExternalUnavailableException raised by a provider's typing wrapper, so it is typed as the
        // common Exception supertype here and rethrown preserving its concrete type in FailingAfterNStream.
        private final Exception midReadFailure;
        private final boolean alwaysFail;
        private int opens = 0;

        MidReadFailingStorageObject(StoragePath path, byte[] payload, int failAfterBytes, Exception midReadFailure) {
            this(path, payload, failAfterBytes, midReadFailure, false);
        }

        MidReadFailingStorageObject(StoragePath path, byte[] payload, int failAfterBytes, Exception midReadFailure, boolean alwaysFail) {
            this.path = path;
            this.payload = payload;
            this.failAfterBytes = failAfterBytes;
            this.midReadFailure = midReadFailure;
            this.alwaysFail = alwaysFail;
        }

        int openCount() {
            return opens;
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = Math.toIntExact(position);
            int remaining = payload.length - pos;
            int len = length == READ_TO_END ? remaining : Math.toIntExact(Math.min(length, remaining));
            byte[] slice = Arrays.copyOfRange(payload, pos, pos + len);
            boolean fail = alwaysFail || opens == 0;
            opens++;
            return fail ? new FailingAfterNStream(slice, failAfterBytes, midReadFailure) : new ByteArrayInputStream(slice);
        }

        @Override
        public InputStream newStream() {
            return newStream(0, payload.length);
        }

        @Override
        public long length() {
            return payload.length;
        }

        @Override
        public Instant lastModified() {
            return null;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return new StorageObjectMetrics(opens, 0, 0, 0);
        }
    }

    /**
     * Delivers {@code chunkBytes} of progress then drops, for the first {@code faults} opens; the next open
     * succeeds with the remaining slice. Models several stacked mid-read resets that each make progress.
     */
    private static final class ChunkedFaultingStorageObject implements StorageObject {
        private final StoragePath path;
        private final byte[] payload;
        private final int chunkBytes;
        private final int faults;
        private final IOException failure;
        private int opens = 0;

        ChunkedFaultingStorageObject(StoragePath path, byte[] payload, int chunkBytes, int faults, IOException failure) {
            this.path = path;
            this.payload = payload;
            this.chunkBytes = chunkBytes;
            this.faults = faults;
            this.failure = failure;
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = Math.toIntExact(position);
            int remaining = payload.length - pos;
            int len = length == READ_TO_END ? remaining : Math.toIntExact(Math.min(length, remaining));
            byte[] slice = Arrays.copyOfRange(payload, pos, pos + len);
            boolean fail = opens < faults;
            opens++;
            return fail ? new FailingAfterNStream(slice, chunkBytes, failure) : new ByteArrayInputStream(slice);
        }

        @Override
        public InputStream newStream() {
            return newStream(0, payload.length);
        }

        @Override
        public long length() {
            return payload.length;
        }

        @Override
        public Instant lastModified() {
            return null;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObjectMetrics metrics() {
            return new StorageObjectMetrics(opens, 0, 0, 0);
        }
    }

    /**
     * Delivers up to {@code failAfter} bytes of {@code data}, then throws {@code failure} on the next read. The
     * fault is either a checked transport {@link IOException} or the unchecked {@link ExternalUnavailableException}
     * raised by a provider's mid-read typing wrapper; {@link #throwFailure()} rethrows it preserving its concrete
     * type, so the resume loop classifies it exactly as it would in production.
     */
    private static final class FailingAfterNStream extends InputStream {
        private final byte[] data;
        private final int failAfter;
        private final Exception failure;
        private int pos = 0;

        FailingAfterNStream(byte[] data, int failAfter, Exception failure) {
            this.data = data;
            this.failAfter = failAfter;
            this.failure = failure;
        }

        private int throwFailure() throws IOException {
            if (failure instanceof IOException io) {
                throw io;
            }
            throw (RuntimeException) failure;
        }

        @Override
        public int read() throws IOException {
            if (pos >= failAfter) {
                return throwFailure();
            }
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (pos >= failAfter) {
                return throwFailure();
            }
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, Math.min(failAfter, data.length) - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }
    }
}
