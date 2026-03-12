/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCursor;
import org.elasticsearch.xpack.esql.action.EsqlCursorAction;
import org.elasticsearch.xpack.esql.action.EsqlCursorRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

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

            cursorIndexService.getMetadata(cursor.cursorId(), listener.delegateFailureAndWrap((metaDelegate, metadata) -> {
                fetchPageWithRetry(cursor, metadata, 0, metaDelegate);
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Attempts to fetch a page from the cursor index, retrying on {@link ResourceNotFoundException}
     * because remaining pages may still be in flight from the async bulk write.
     */
    private void fetchPageWithRetry(
        EsqlCursor cursor,
        EsqlCursorIndexService.CursorMetadata metadata,
        int attempt,
        ActionListener<EsqlQueryResponse> listener
    ) {
        cursorIndexService.getPage(cursor.cursorId(), cursor.pageIndex(), ActionListener.wrap(pages -> {
            String nextCursor = null;
            int nextPageIndex = cursor.pageIndex() + 1;
            if (nextPageIndex < metadata.totalPages()) {
                nextCursor = new EsqlCursor(cursor.cursorId(), nextPageIndex, cursor.pageSize()).encode();
            }

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
            if (e instanceof ResourceNotFoundException && attempt < MAX_PAGE_FETCH_RETRIES) {
                logger.debug(
                    "page [{}] for cursor [{}] not yet available, retrying (attempt [{}])",
                    cursor.pageIndex(),
                    cursor.cursorId(),
                    attempt + 1
                );
                threadPool.schedule(
                    () -> fetchPageWithRetry(cursor, metadata, attempt + 1, listener),
                    PAGE_FETCH_RETRY_DELAY,
                    threadPool.generic()
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }
}
