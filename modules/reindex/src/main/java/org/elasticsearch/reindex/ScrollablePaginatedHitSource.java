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
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Scroll-based pagination. Uses scroll_id to fetch subsequent batches.
 */
public abstract class ScrollablePaginatedHitSource extends PaginatedHitSource {

    private final AtomicReference<String> scrollId = new AtomicReference<>();

    public ScrollablePaginatedHitSource(
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
        if (resumeInfo instanceof ScrollWorkerResumeInfo == false) {
            throw new IllegalArgumentException(
                "ScrollablePaginatedHitSource requires ScrollWorkerResumeInfo, got " + resumeInfo.getClass()
            );
        }
        restoreScrollState((ScrollWorkerResumeInfo) resumeInfo);
    }

    @Override
    protected final PaginationCursor getCursorForNextBatch() {
        String sid = scrollId.get();
        return Strings.hasLength(sid) ? PaginationCursor.forScroll(sid) : null;
    }

    @Override
    protected final void onBatchResponse(Response response) {
        scrollId.set(response.getScrollId());
    }

    @Override
    public final boolean hasMoreBatches() {
        return Strings.hasLength(scrollId.get());
    }

    @Override
    protected final void doNextSearch(
        PaginationCursor cursor,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    ) {
        if (cursor.isScroll() == false) {
            throw new IllegalStateException("ScrollablePaginatedHitSource expects scroll cursor");
        }
        doNextScrollSearch(cursor.scrollId(), extraKeepAlive, searchListener);
    }

    protected final String getScrollId() {
        return scrollId.get();
    }

    /**
     * Set the scroll id. Used for resume and by tests.
     */
    public final void setScrollId(String sid) {
        scrollId.set(sid);
    }

    protected abstract void restoreScrollState(ScrollWorkerResumeInfo resumeInfo);

    protected abstract void doNextScrollSearch(
        String scrollId,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    );
}
