/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the external-format page iterators (NDJSON, CSV, parquet-rs) that buffer a single look-ahead
 * {@link Page} in {@link #nextPage} — materialized by the subclass's {@code hasNext()} and handed out (and
 * nulled) by its {@code next()}.
 * <p>
 * It centralizes the one thing every such iterator must do and each previously forgot: <b>release that buffered
 * page on {@link #close()}</b>. When a consumer closes early — a pushed-down {@code LIMIT}, a query
 * cancellation, or a downstream operator error — {@code hasNext()} may have already materialized a page that
 * {@code next()} never consumed; without this release its {@link org.elasticsearch.compute.data.Block}s leak
 * against the circuit breaker. Subclasses keep their own produce/consume logic and put their teardown in
 * {@link #closeInternal()}.
 * <p>
 * Threading contract: iteration ({@code hasNext()}/{@code next()}) is single-threaded — one consumer drives
 * the iterator and is the only writer of {@link #nextPage}. {@code close()} is idempotent and thread-safe
 * (first caller wins) so it may run on a different thread than iteration: a consumer hands the iterator off to
 * a cancel/abort path, and some backends (e.g. the native parquet-rs reader) must not be torn down twice. The
 * buffered page is published to the closing thread by the same handoff that transfers ownership. Subclasses
 * must preserve the single-threaded-iteration precondition; {@code nextPage} is not safe to publish across
 * concurrent iterators.
 */
public abstract class BufferingPageIterator implements CloseableIterator<Page> {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * The single look-ahead page: set by the subclass's {@code hasNext()}, handed out (and nulled) by
     * {@code next()}. Volatile so {@link #close()} — which may run on a different thread than iteration — reads
     * the iterating thread's last write rather than a stale value, closing the publish gap for the off-thread
     * close the class contract permits.
     */
    protected volatile Page nextPage;

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        try {
            Page buffered = nextPage;
            nextPage = null;
            if (buffered != null) {
                buffered.releaseBlocks();
            }
        } finally {
            closeInternal();
        }
    }

    /**
     * Whether {@link #close()} has been entered. Lets a subclass's {@code hasNext()} short-circuit to
     * {@code false} after close instead of touching an already-torn-down decoder / native handle.
     */
    protected final boolean isClosed() {
        return closed.get();
    }

    /**
     * Subclass teardown: close streams / decoders / native handles and publish any end-of-read stats. Invoked
     * exactly once, after the buffered page (if any) has been released, even if that release throws.
     */
    protected abstract void closeInternal() throws IOException;
}
