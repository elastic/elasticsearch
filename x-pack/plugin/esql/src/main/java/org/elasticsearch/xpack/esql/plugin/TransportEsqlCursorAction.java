/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCursor;
import org.elasticsearch.xpack.esql.action.EsqlCursorAction;
import org.elasticsearch.xpack.esql.action.EsqlCursorRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.List;

public class TransportEsqlCursorAction extends HandledTransportAction<EsqlCursorRequest, EsqlQueryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportEsqlCursorAction.class);

    static final int MAX_PAGE_FETCH_RETRIES = 10;
    static final TimeValue PAGE_FETCH_RETRY_DELAY = TimeValue.timeValueMillis(200);

    private final EsqlCursorIndexService cursorIndexService;
    private final ThreadPool threadPool;

    @Inject
    public TransportEsqlCursorAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EsqlCursorIndexService cursorIndexService
    ) {
        super(EsqlCursorAction.NAME, transportService, actionFilters, EsqlCursorRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.cursorIndexService = cursorIndexService;
        this.threadPool = transportService.getThreadPool();
    }

    @Override
    protected void doExecute(Task task, EsqlCursorRequest request, ActionListener<EsqlQueryResponse> listener) {
        listener = listener.delegateFailureAndWrap(ActionListener::respondAndRelease);
        try {
            EsqlCursor cursor = EsqlCursor.decode(request.cursor());

            if (request.keepAlive() != null) {
                cursorIndexService.updateKeepAlive(cursor.cursorId(), request.keepAlive(), ActionListener.noop());
            }

            getMetadataWithRetry(cursor, 0, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void getMetadataWithRetry(EsqlCursor cursor, int attempt, ActionListener<EsqlQueryResponse> listener) {
        cursorIndexService.getMetadata(cursor.cursorId(), ActionListener.wrap(metadata -> {
            if (metadata.isFailed()) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "cursor [%s] query failed during execution",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        cursor.cursorId()
                    )
                );
                return;
            }
            if (metadata.isComplete() && cursor.pageIndex() >= metadata.totalPages()) {
                listener.onResponse(emptyResponse(metadata));
                return;
            }
            fetchPageWithRetry(cursor, metadata, 0, listener);
        }, e -> {
            if (isRetryable(e) && attempt < MAX_PAGE_FETCH_RETRIES) {
                logger.debug(
                    "cursor index not yet available for metadata fetch of [{}], retrying (attempt [{}])",
                    cursor.cursorId(),
                    attempt + 1
                );
                threadPool.schedule(
                    () -> getMetadataWithRetry(cursor, attempt + 1, listener),
                    PAGE_FETCH_RETRY_DELAY,
                    threadPool.generic()
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Attempts to fetch a page from the cursor index, retrying on {@link ResourceNotFoundException}
     * because the page may still be in flight from the incremental store operator or async bulk write.
     * <p>
     * When the cursor is in-progress ({@link EsqlCursorIndexService#TOTAL_PAGES_IN_PROGRESS}), retries re-fetch metadata to detect
     * when the query completes or fails.
     */
    private void fetchPageWithRetry(
        EsqlCursor cursor,
        EsqlCursorIndexService.CursorMetadata metadata,
        int attempt,
        ActionListener<EsqlQueryResponse> listener
    ) {
        cursorIndexService.getPage(cursor.cursorId(), cursor.pageIndex(), ActionListener.wrap(pages -> {
            String nextCursor = computeNextCursor(cursor, metadata);

            listener.onResponse(
                new EsqlQueryResponse(
                    metadata.columns(),
                    pages,
                    0,
                    0,
                    null,
                    metadata.columnar(),
                    null,
                    false,
                    false,
                    metadata.zoneId(),
                    0,
                    0,
                    null,
                    nextCursor
                )
            );
        }, e -> {
            if (isRetryable(e) && attempt < MAX_PAGE_FETCH_RETRIES) {
                logger.debug(
                    "page [{}] for cursor [{}] not yet available, retrying (attempt [{}])",
                    cursor.pageIndex(),
                    cursor.cursorId(),
                    attempt + 1
                );
                threadPool.schedule(
                    () -> retryWithFreshMetadata(cursor, metadata, attempt, listener),
                    PAGE_FETCH_RETRY_DELAY,
                    threadPool.generic()
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * On retry, re-fetch metadata if the cursor was in-progress so we can detect completion or failure.
     * For completed cursors, reuse the existing metadata.
     */
    private void retryWithFreshMetadata(
        EsqlCursor cursor,
        EsqlCursorIndexService.CursorMetadata currentMetadata,
        int attempt,
        ActionListener<EsqlQueryResponse> listener
    ) {
        if (currentMetadata.isInProgress()) {
            cursorIndexService.getMetadata(cursor.cursorId(), ActionListener.wrap(freshMetadata -> {
                if (freshMetadata.isFailed()) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "cursor [%s] query failed during execution",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            cursor.cursorId()
                        )
                    );
                    return;
                }
                if (freshMetadata.isComplete() && cursor.pageIndex() >= freshMetadata.totalPages()) {
                    listener.onResponse(emptyResponse(freshMetadata));
                    return;
                }
                fetchPageWithRetry(cursor, freshMetadata, attempt + 1, listener);
            }, e -> {
                logger.warn("failed to re-fetch metadata for cursor [{}] on retry", cursor.cursorId(), e);
                fetchPageWithRetry(cursor, currentMetadata, attempt + 1, listener);
            }));
        } else {
            fetchPageWithRetry(cursor, currentMetadata, attempt + 1, listener);
        }
    }

    private static EsqlQueryResponse emptyResponse(EsqlCursorIndexService.CursorMetadata metadata) {
        return new EsqlQueryResponse(
            metadata.columns(),
            List.of(),
            0,
            0,
            null,
            metadata.columnar(),
            null,
            false,
            false,
            metadata.zoneId(),
            0,
            0,
            null,
            null
        );
    }

    /**
     * Determines the next cursor token, or {@code null} if this is the last page.
     * <p>
     * For in-progress cursors we always return a next cursor since we don't know the total yet.
     * For complete cursors we check against {@code totalPages}.
     */
    private static String computeNextCursor(EsqlCursor cursor, EsqlCursorIndexService.CursorMetadata metadata) {
        int nextPageIndex = cursor.pageIndex() + 1;
        if (metadata.isInProgress() || nextPageIndex < metadata.totalPages()) {
            return new EsqlCursor(cursor.cursorId(), nextPageIndex).encode();
        }
        return null;
    }

    static boolean isRetryable(Exception e) {
        return e instanceof ResourceNotFoundException || e instanceof NoShardAvailableActionException;
    }
}
