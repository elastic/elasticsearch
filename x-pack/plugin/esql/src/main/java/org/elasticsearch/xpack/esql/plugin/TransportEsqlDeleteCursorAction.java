/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCursor;
import org.elasticsearch.xpack.esql.action.EsqlDeleteCursorAction;
import org.elasticsearch.xpack.esql.action.EsqlDeleteCursorRequest;

public class TransportEsqlDeleteCursorAction extends HandledTransportAction<EsqlDeleteCursorRequest, AcknowledgedResponse> {

    private final EsqlCursorStore cursorStore;

    @Inject
    public TransportEsqlDeleteCursorAction(TransportService transportService, ActionFilters actionFilters, EsqlCursorStore cursorStore) {
        super(
            EsqlDeleteCursorAction.NAME,
            transportService,
            actionFilters,
            EsqlDeleteCursorRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.cursorStore = cursorStore;
    }

    @Override
    protected void doExecute(Task task, EsqlDeleteCursorRequest request, ActionListener<AcknowledgedResponse> listener) {
        try {
            EsqlCursor cursor = EsqlCursor.decode(request.cursor());
            boolean deleted = cursorStore.delete(cursor.cursorId());
            listener.onResponse(AcknowledgedResponse.of(deleted));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
