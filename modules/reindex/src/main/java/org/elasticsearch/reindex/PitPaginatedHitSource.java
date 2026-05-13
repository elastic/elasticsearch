/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * PIT-based pagination. Uses search_after to fetch subsequent batches.
 */
public abstract class PitPaginatedHitSource extends PaginatedHitSource {

    private final AtomicReference<Object[]> searchAfterValues = new AtomicReference<>();

    /// Total hit count from the first batch, or `null` until the first batch has been observed. Subsequent
    /// batches disable `track_total_hits` and reuse this cached value to keep
    /// [org.elasticsearch.index.reindex.BulkByScrollTask.Status#getTotal()] accurate.
    ///
    /// Precondition: the first batch must run with accurate `track_total_hits` for this cache to be
    /// meaningful. Reindex's `prepareSearchRequest` enforces this for PIT searches.
    private final AtomicReference<Long> cachedTotalHits = new AtomicReference<>();

    public PitPaginatedHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
    }

    @Override
    protected final void restoreState(WorkerResumeInfo resumeInfo) {
        if (resumeInfo instanceof PitWorkerResumeInfo == false) {
            throw new IllegalArgumentException("PitPaginatedHitSource requires PitWorkerResumeInfo, got " + resumeInfo.getClass());
        }
        restorePitState((PitWorkerResumeInfo) resumeInfo);
    }

    @Override
    protected final PaginationCursor getCursorForNextBatch() {
        Object[] sa = searchAfterValues.get();
        return sa != null ? PaginationCursor.forSearchAfter(sa) : null;
    }

    @Override
    protected final void onBatchResponse(Response response) {
        searchAfterValues.set(response.getSearchAfterValues());
        // Only the first batch runs with accurate track_total_hits, so cache it once and ignore later batches.
        cachedTotalHits.compareAndSet(null, response.getTotalHits());
    }

    /// Total hits captured from the first batch, or `null` before the first batch has been observed.
    @Nullable
    protected final Long getCachedTotalHits() {
        return cachedTotalHits.get();
    }

    @Override
    public final boolean hasMoreBatches() {
        return searchAfterValues.get() != null;
    }

    @Override
    protected final void doNextSearch(
        PaginationCursor cursor,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    ) {
        if (cursor.isSearchAfter() == false) {
            throw new IllegalStateException("PitPaginatedHitSource expects search_after cursor");
        }
        doNextPitSearch(cursor.searchAfter(), extraKeepAlive, searchListener);
    }

    @Override
    protected final void releaseSearchContext(Runnable onCompletion) {
        onCompletion.run();
    }

    /**
     * Set the search_after values. Used for resume and by tests.
     */
    public final void setSearchAfterValues(Object[] values) {
        searchAfterValues.set(values);
    }

    /**
     * Returns the current PIT ID from the last search response that included one.
     * Used when closing the PIT to ensure we close the most recent context.
     */
    public abstract BytesReference getPitId();

    protected abstract void restorePitState(PitWorkerResumeInfo resumeInfo);

    protected abstract void doNextPitSearch(
        Object[] searchAfter,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    );
}
