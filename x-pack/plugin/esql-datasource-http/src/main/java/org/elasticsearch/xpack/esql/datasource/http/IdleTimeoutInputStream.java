/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unblocks a stuck body {@code read} by closing the delegate when no bytes arrive within
 * {@code idleTimeout}. JDK {@code HttpClient} exposes no socket/read timeout on
 * {@code BodyHandlers.ofInputStream()} after headers, so a peer that keeps the connection open
 * and sends nothing would otherwise pin the caller forever. Closing from a watchdog task is the
 * documented way to abort that blocking read; the next {@code read} then fails with
 * {@link SocketTimeoutException}, which the retry layer already treats as transient — matching S3's
 * ~30 s idle socket timeout.
 * <p>
 * The timer measures a <em>gap between progress</em>, not a bound on the whole transfer.
 * {@link #skip(long)} therefore discards in {@link #SKIP_CHUNK}-sized steps so a 200-fallback
 * prefix skip on a live but slow body keeps resetting the watchdog as bytes flow. A single
 * {@code delegate.skip(position)} would treat the whole seek as one idle interval and abort a
 * connection that never went silent.
 * <p>
 * The watchdog is a delayed task on a {@link ScheduledExecutorService} (one daemon thread owned by
 * {@code HttpStorageProvider}). {@link FutureUtils#cancel} removes it from the delay queue without
 * interrupting a pool thread. {@link Duration#ZERO} or a null scheduler disables the timer.
 */
final class IdleTimeoutInputStream extends FilterInputStream {

    /**
     * Upper bound on one {@code skip}/{@code read} fallback step. 8 KiB at ~1 KiB/s (the retry
     * layer's drip floor) finishes in ~8 s, inside the default 30 s idle timeout, so a slow-but-steady
     * prefix skip is not mistaken for a stall.
     */
    static final int SKIP_CHUNK = 8192;

    private final Duration idleTimeout;
    private final StoragePath path;
    private final ScheduledExecutorService scheduler;

    static InputStream wrap(InputStream body, Duration idleTimeout, StoragePath path, ScheduledExecutorService scheduler) {
        if (body == null) {
            throw new IllegalArgumentException("body cannot be null");
        }
        if (idleTimeout == null) {
            throw new IllegalArgumentException("idleTimeout cannot be null");
        }
        if (idleTimeout.isNegative()) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        if (idleTimeout.isZero() || scheduler == null) {
            return body;
        }
        return new IdleTimeoutInputStream(body, idleTimeout, path, scheduler);
    }

    private IdleTimeoutInputStream(InputStream body, Duration idleTimeout, StoragePath path, ScheduledExecutorService scheduler) {
        super(body);
        this.idleTimeout = idleTimeout;
        this.path = path;
        this.scheduler = scheduler;
    }

    @Override
    public int read() throws IOException {
        return (int) withIdleTimeout(() -> in.read());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return (int) withIdleTimeout(() -> in.read(b, off, len));
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }
        long skipped = 0;
        byte[] buf = new byte[SKIP_CHUNK];
        while (skipped < n) {
            long remaining = n - skipped;
            long chunk = Math.min(remaining, SKIP_CHUNK);
            long s = withIdleTimeout(() -> in.skip(chunk));
            if (s > 0) {
                skipped += s;
                continue;
            }
            // skip() may return 0 without EOF; fall back to a chunked read so the idle timer still
            // resets per buffer of discarded bytes.
            int toRead = (int) Math.min(buf.length, remaining);
            int r = (int) withIdleTimeout(() -> in.read(buf, 0, toRead));
            if (r <= 0) {
                break;
            }
            skipped += r;
        }
        return skipped;
    }

    /**
     * Arms a delayed closer for {@code idleTimeout}, runs {@code op}, then cancels the closer.
     * If the closer wins, the delegate is closed so a blocked {@code op} unblocks, and this
     * throws {@link SocketTimeoutException}.
     */
    private long withIdleTimeout(IOLongOp op) throws IOException {
        AtomicBoolean finished = new AtomicBoolean();
        ScheduledFuture<?> watchdog = scheduler.schedule(() -> {
            if (finished.compareAndSet(false, true)) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // Closing is the abort signal; a close fault must not hide the timeout.
                }
            }
        }, idleTimeout.toNanos(), TimeUnit.NANOSECONDS);
        try {
            long n = op.run();
            if (finished.compareAndSet(false, true) == false) {
                throw timeoutException(null);
            }
            return n;
        } catch (IOException e) {
            if (finished.compareAndSet(false, true) == false) {
                throw timeoutException(e);
            }
            throw e;
        } finally {
            FutureUtils.cancel(watchdog);
        }
    }

    private SocketTimeoutException timeoutException(IOException cause) {
        SocketTimeoutException timeout = new SocketTimeoutException("Idle timeout after " + idleTimeout + " reading " + path);
        if (cause != null) {
            timeout.initCause(cause);
        }
        return timeout;
    }

    @FunctionalInterface
    private interface IOLongOp {
        long run() throws IOException;
    }
}
