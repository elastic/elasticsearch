/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * Wraps a {@link StorageObject} with retry logic for transient storage failures.
 * Retries are applied to all I/O operations (stream open, metadata, async reads)
 * using exponential backoff with jitter. Throttling errors (429/503) get a higher
 * retry budget than other transient errors.
 */
class RetryableStorageObject implements StorageObject {

    private static final Logger logger = LogManager.getLogger(RetryableStorageObject.class);

    /**
     * Absolute backstop on how many times a single range read may re-open and resume, independent of the
     * per-episode retry budget. Bounds the degenerate "trickle a few bytes, then drop, forever" case; a real
     * range read completes in a handful of re-opens at most, so this is never approached in practice.
     */
    private static final int MAX_TOTAL_RESUMES = 1000;

    private final StorageObject delegate;
    private final RetryPolicy retryPolicy;
    /** Schedules the async read-retry continuation after a backoff delay without parking a thread on the wait. */
    private final RetryScheduler retryScheduler;
    /**
     * Local counter for retries observed at this decorator boundary, merged into {@link #metrics()}
     * via {@link StorageObjectMetrics#add}.
     * <p>
     * <b>Invariant:</b> only {@link StorageObjectMetricsCounters#addRetry()} may be called on this
     * instance. The delegate already counts requests / request-nanos / bytes-read; calling
     * {@code addRequest} here would double-count those into the merged snapshot.
     */
    private final StorageObjectMetricsCounters retryCounters = new StorageObjectMetricsCounters();

    RetryableStorageObject(StorageObject delegate, RetryPolicy retryPolicy) {
        this(delegate, retryPolicy, RetryScheduler.DIRECT);
    }

    RetryableStorageObject(StorageObject delegate, RetryPolicy retryPolicy, RetryScheduler retryScheduler) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        if (retryScheduler == null) {
            throw new IllegalArgumentException("retryScheduler cannot be null");
        }
        this.delegate = delegate;
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
    }

    @Override
    public InputStream newStream() throws IOException {
        // Whole-object open via the delegate's whole-object read — no length() lookup, so it works for streams
        // with no known length (e.g. compressed objects). Wrapped so a transient fault DURING the read re-opens
        // the undelivered tail [delivered, end] as an open-ended (READ_TO_END) range and resumes, byte-exact.
        // This gives single-file text, compressed text and the ORC seed stream the same resume as ranged reads,
        // for any provider that surfaces a mid-read fault as a (transient-classified) IOException. S3, GCS, Azure
        // and HTTP all do so via their per-provider mid-read typing wrappers (TransientTypingInputStream /
        // GcsTransientTypingInputStream / AzureTransientTypingInputStream / HttpTransientTypingInputStream) — HTTP
        // needs one because a content-length-short premature EOF throws a plain IOException the classifier would
        // otherwise treat as a hard error. Local file reads surface plain IOExceptions and are intentionally NOT
        // typed: a local-disk read fault is a genuine hard error, not a transient transport drop. In production
        // the decompressor sits ABOVE this layer, so a resume re-opens the (length-bearing) compressed bytes.
        InputStream initial = retryPolicy.execute(delegate::newStream, "newStream", delegate.path(), retryCounters::addRetry);
        return new ResumingInputStream(initial, 0, READ_TO_END);
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        // Retry the OPEN (existing behavior), then wrap the stream so a transient fault DURING the read
        // re-opens the remaining byte range and resumes — byte-exact, governed by the same RetryPolicy.
        // This is the path text segments and parquet row-groups/footers read through, so closing the
        // mid-read gap here makes every format resilient without per-format code.
        InputStream initial = retryPolicy.execute(
            () -> delegate.newStream(position, length),
            "newStream(range)",
            delegate.path(),
            retryCounters::addRetry
        );
        return new ResumingInputStream(initial, position, length);
    }

    @Override
    public long length() throws IOException {
        return retryPolicy.execute(delegate::length, "length", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public Instant lastModified() throws IOException {
        return retryPolicy.execute(delegate::lastModified, "lastModified", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public boolean exists() throws IOException {
        return retryPolicy.execute(delegate::exists, "exists", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        // No retry on abort: the underlying provider's abortStream is a best-effort
        // connection-discard (e.g. S3 ResponseInputStream.abort()). If we silently fall back to
        // the SPI default stream.close() here, providers like S3 drain the entire response body
        // before returning, defeating the purpose of abortStream on partial-read paths.
        //
        // A ResumingInputStream is our own wrapper; abort the live underlying stream so the provider's
        // Abortable fast-path applies to the real instance, not the wrapper (which the provider can't cast).
        if (stream instanceof ResumingInputStream resuming) {
            delegate.abortStream(resuming.currentStream());
        } else {
            delegate.abortStream(stream);
        }
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        int savedPosition = target.position();
        return retryPolicy.execute(() -> {
            target.position(savedPosition);
            return delegate.readBytes(position, target);
        }, "readBytes", delegate.path(), retryCounters::addRetry);
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        readBytesAsyncWithRetry(position, length, factory, executor, listener, 0, System.nanoTime());
    }

    private void readBytesAsyncWithRetry(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener,
        int attempt,
        long startNanos
    ) {
        delegate.readBytesAsync(position, length, factory, executor, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer result) {
                retryPolicy.notifySuccess();
                // Do NOT route a throw from listener.onResponse into onFailure — that would
                // trigger retry logic or double-complete the downstream listener. Propagate
                // the exception directly so the caller's uncaught-exception handler deals with it.
                try {
                    listener.onResponse(result);
                } catch (Exception e) {
                    // listener.onResponse threw before consuming the buffer; release it so the
                    // breaker reservation does not outlive the failed delivery.
                    try {
                        result.close();
                    } catch (Exception closeEx) {
                        e.addSuppressed(closeEx);
                    }
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                // One shared decision point (classify, budget, backoff) for every driver. The delegate has
                // already released its DirectReadBuffer on the failure path, so a retry simply allocates a
                // fresh one via the factory on the next attempt — nothing to release here.
                RetryPolicy.RetryDecision decision = retryPolicy.decide(e, attempt, startNanos);
                if (decision.retry() == false) {
                    listener.onFailure(e);
                    return;
                }
                retryCounters.addRetry();
                logger.debug(
                    "retrying async read for [{}] (attempt [{}], delay [{}]ms): [{}]",
                    delegate.path(),
                    attempt + 1,
                    decision.delayMillis(),
                    e.getMessage()
                );
                // Reschedule the retry as a continuation rather than sleeping: the backoff is honored by the
                // scheduler's timer, not by parking this (general-pool) thread on Thread.sleep for the delay.
                // Tripwire: if the scheduler is torn down mid-backoff the queued continuation is silently dropped
                // and the listener never completes. Benign only because that happens solely at node shutdown,
                // which abandons (not awaits) query futures and reclaims all state on JVM exit. Revisit if
                // graceful query drain is ever added — a stranded listener would then stall shutdown.
                try {
                    retryScheduler.schedule(
                        () -> readBytesAsyncWithRetry(position, length, factory, executor, listener, attempt + 1, startNanos),
                        decision.delayMillis(),
                        executor
                    );
                } catch (Exception rejected) {
                    // Scheduler/executor rejected the retry (e.g. shutdown or saturated queue) — surface it so the
                    // listener is always completed rather than silently dropped.
                    listener.onFailure(rejected);
                }
            }
        });
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics().add(retryCounters.snapshot());
    }

    /**
     * Wraps a range read so a transient transport fault <em>during</em> the read re-opens the remaining byte
     * range and resumes, instead of failing the whole read. Resume is byte-exact: {@code delivered} tracks
     * bytes already handed to the caller, so a re-open requests {@code [position + delivered, end]} and no
     * byte is delivered twice or skipped (object content is immutable for the life of a query). Whether a
     * fault is retryable, the backoff, and the total-time budget all come from the same {@link RetryPolicy}
     * used for opens; a non-retryable fault or an exhausted budget propagates unchanged. Reading raw object
     * bytes, a failure here is almost always transport (parsing happens above this stream); a rare
     * data-integrity error misclassified as transient simply re-trips and fails within the bounded budget.
     * <p>
     * Single-threaded by contract: one consumer reads one stream. Not {@code Abortable}; the enclosing
     * {@link #abortStream} unwraps to abort the live underlying stream.
     */
    private final class ResumingInputStream extends InputStream {
        private final long position;
        private final long length;
        // Volatile: the reader thread re-assigns this on resume while {@link #abortStream} reads it (via
        // currentStream()) from the operator/cancel thread, so the abort must see the live stream, not a stale ref.
        private volatile InputStream current;
        private long delivered = 0;

        ResumingInputStream(InputStream initial, long position, long length) {
            this.current = initial;
            this.position = position;
            this.length = length;
        }

        // Consecutive re-opens since the last byte of progress, and when that "stuck" episode began.
        // Both reset whenever a read delivers bytes: a stream that keeps making progress (even if it drops
        // repeatedly) is not stuck and completes; only a stream stuck at the same offset exhausts the budget.
        private int failuresSinceProgress = 0;
        private long episodeStartNanos = 0;
        // Backstop independent of progress: a single range read that re-opens this many times — even while
        // delivering a trickle of bytes each time — is treated as failed, bounding the degenerate slow-drip
        // case that the per-episode budget alone would let loop. A genuine range read never approaches this.
        private int totalResumes = 0;

        InputStream currentStream() {
            return current;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            while (true) {
                try {
                    int n = current.read(b, off, len);
                    if (n > 0) {
                        delivered += n;
                        failuresSinceProgress = 0;
                        episodeStartNanos = 0;
                    }
                    return n;
                } catch (IOException | ExternalUnavailableException e) {
                    // A raw transport fault surfaces as an IOException; a provider's typing wrapper re-types a
                    // mid-read status fault as the unchecked ExternalUnavailableException. Both drive a resume.
                    reopenOrThrow(e);
                }
            }
        }

        private void reopenOrThrow(Exception e) throws IOException {
            if (totalResumes >= MAX_TOTAL_RESUMES) {
                throw rethrow(e);
            }
            if (episodeStartNanos == 0) {
                episodeStartNanos = System.nanoTime();
            }
            // One shared decision: classify, apply the per-episode budget (failuresSinceProgress, which resets on
            // byte progress), feed adaptive backoff on a throttle, and check the episode time budget.
            RetryPolicy.RetryDecision decision = retryPolicy.decide(e, failuresSinceProgress, episodeStartNanos);
            if (decision.retry() == false) {
                throw rethrow(e);
            }
            closeQuietly(current);
            if (decision.delayMillis() > 0) {
                try {
                    Thread.sleep(decision.delayMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while waiting to resume read of " + delegate.path(), ie);
                }
            }
            retryCounters.addRetry();
            failuresSinceProgress++;
            totalResumes++;
            long resumeFrom = position + delivered;
            logger.debug(
                "resuming read of [{}] from byte [{}] after transient fault (attempt [{}]): [{}]",
                delegate.path(),
                resumeFrom,
                failuresSinceProgress,
                e.getMessage()
            );
            // Re-open the undelivered tail THROUGH the open-retry loop, so a transient failure to re-open the
            // range (not just to read it) is itself retried.
            if (length == READ_TO_END) {
                // Open-ended (to-EOF) mode: re-open [resumeFrom, end] as an open-ended range; the underlying
                // stream's EOF marks completion. If the fault landed exactly at EOF, the provider answers the
                // past-the-end open-ended read with an empty stream.
                current = retryPolicy.execute(
                    () -> delegate.newStream(resumeFrom, READ_TO_END),
                    "newStream(resume-open)",
                    delegate.path(),
                    retryCounters::addRetry
                );
            } else {
                long remaining = length - delivered;
                // If everything was delivered, an empty stream is EOF.
                current = remaining > 0
                    ? retryPolicy.execute(
                        () -> delegate.newStream(resumeFrom, remaining),
                        "newStream(resume)",
                        delegate.path(),
                        retryCounters::addRetry
                    )
                    : InputStream.nullInputStream();
            }
        }

        @Override
        public int available() throws IOException {
            return current.available();
        }

        @Override
        public void close() throws IOException {
            current.close();
        }

        private void closeQuietly(InputStream in) {
            // Best-effort discard before re-opening a fresh range. Swallow both a checked IOException and an
            // unchecked close failure (e.g. the AWS SDK's AbortedException) so a noisy close never masks the resume.
            IOUtils.closeWhileHandlingException(in);
        }

        /**
         * Rethrows the caught fault preserving its type, on the give-up path. The resume loop only ever catches an
         * {@link IOException} (raw transport) or the unchecked {@link ExternalUnavailableException} (a typed status
         * fault), so the cast is safe. Declares a return type so callers can write {@code throw rethrow(e)} and the
         * compiler sees an exit.
         */
        private static RuntimeException rethrow(Exception e) throws IOException {
            if (e instanceof IOException io) {
                throw io;
            }
            return (RuntimeException) e;
        }
    }
}
