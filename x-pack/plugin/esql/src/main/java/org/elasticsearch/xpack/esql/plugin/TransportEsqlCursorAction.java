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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCursor;
import org.elasticsearch.xpack.esql.action.EsqlCursorAction;
import org.elasticsearch.xpack.esql.action.EsqlCursorRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.List;

public class TransportEsqlCursorAction extends HandledTransportAction<EsqlCursorRequest, EsqlQueryResponse> {

    private final EsqlCursorStore cursorStore;

    @Inject
    public TransportEsqlCursorAction(TransportService transportService, ActionFilters actionFilters, EsqlCursorStore cursorStore) {
        super(EsqlCursorAction.NAME, transportService, actionFilters, EsqlCursorRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.cursorStore = cursorStore;
    }

    @Override
    protected void doExecute(Task task, EsqlCursorRequest request, ActionListener<EsqlQueryResponse> listener) {
        listener = listener.delegateFailureAndWrap(ActionListener::respondAndRelease);
        try {
            EsqlCursor cursor = EsqlCursor.decode(request.cursor());
            EsqlCursorStore.CursorState state = cursorStore.get(cursor.cursorId());
            if (state == null) {
                listener.onFailure(new ResourceNotFoundException("cursor not found or expired"));
                return;
            }

            if (request.keepAlive() != null) {
                cursorStore.updateKeepAlive(cursor.cursorId(), request.keepAlive());
            }

            int pageSize = cursor.pageSize();
            int fromRow = cursor.nextRowOffset();
            int totalRows = state.totalRows();

            List<Page> responsePages = TransportEsqlQueryAction.slicePages(state.pages(), fromRow, pageSize);

            String nextCursor = null;
            int nextRowOffset = fromRow + pageSize;
            if (nextRowOffset < totalRows) {
                nextCursor = new EsqlCursor(cursor.cursorId(), nextRowOffset, pageSize).encode();
            }

            listener.onResponse(
                new EsqlQueryResponse(
                    state.columns(),
                    responsePages,
                    0,
                    0,
                    null,
                    state.columnar(),
                    null,
                    false,
                    false,
                    state.zoneId(),
                    0,
                    0,
                    null,
                    nextCursor
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
