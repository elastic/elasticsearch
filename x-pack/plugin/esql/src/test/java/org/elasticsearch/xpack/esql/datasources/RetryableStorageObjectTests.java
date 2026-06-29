/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
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

import static org.hamcrest.Matchers.lessThan;
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
