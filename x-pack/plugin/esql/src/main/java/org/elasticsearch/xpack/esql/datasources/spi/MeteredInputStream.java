/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Counts physical bytes delivered by a leaf {@link StorageObject#newStream} into the profile
 * {@link StorageObjectMetricsCounters} and publishes one APM bytes event at close or abort.
 * <p>
 * The tally is a plain {@code long} on the producer thread — no per-byte atomics. Live
 * {@code _tasks} snapshots may trail by one {@link #PUBLISH_CHUNK_BYTES} chunk; close/abort
 * flushes the remainder so the query profile is exact. Abort-in-flight slack (bytes sitting
 * in an SDK buffer that never reached {@code read}) is unobservable and excluded.
 * <p>
 * {@link #abort()} publishes the same totals, then runs the optional {@code onAbort} callback
 * instead of {@link InputStream#close()}. S3 sets that callback so the HTTP connection is
 * discarded without draining; other providers omit it and abort falls through to close.
 */
public final class MeteredInputStream extends FilterInputStream {

    /** How many newly received bytes accumulate before a live profile publish. */
    static final int PUBLISH_CHUNK_BYTES = 256 * 1024;

    private final StorageObjectMetricsCounters counters;
    private final Runnable onAbort;
    private final AtomicBoolean closed = new AtomicBoolean();

    private long local;
    private long lastPublished;

    public MeteredInputStream(InputStream in, StorageObjectMetricsCounters counters) {
        this(in, counters, null);
    }

    /**
     * @param onAbort if non-null, {@link #abort()} runs this instead of {@code in.close()}
     */
    public MeteredInputStream(InputStream in, StorageObjectMetricsCounters counters, Runnable onAbort) {
        super(in);
        this.counters = counters;
        this.onAbort = onAbort;
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (b >= 0) {
            account(1L);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = in.read(b, off, len);
        if (n > 0) {
            account(n);
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = in.skip(n);
        if (skipped > 0) {
            account(skipped);
        }
        return skipped;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readlimit) {
        // Received-byte accounting cannot follow a reset; refuse mark so callers do not double-count.
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("mark/reset not supported by MeteredInputStream");
    }

    /**
     * Flushes remaining bytes and publishes the stream total once, then aborts the inner
     * stream when {@code onAbort} was set, otherwise closes it. Idempotent with {@link #close()}.
     */
    public void abort() throws IOException {
        finish(true);
    }

    @Override
    public void close() throws IOException {
        finish(false);
    }

    private void account(long n) {
        local += n;
        long unpublished = local - lastPublished;
        if (unpublished >= PUBLISH_CHUNK_BYTES) {
            counters.addBytes(unpublished);
            lastPublished = local;
        }
    }

    private void finish(boolean abort) throws IOException {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        long remainder = local - lastPublished;
        if (remainder > 0) {
            counters.addBytes(remainder);
            lastPublished = local;
        }
        counters.publishStreamBytes(local);
        if (abort && onAbort != null) {
            onAbort.run();
        } else {
            in.close();
        }
    }
}
