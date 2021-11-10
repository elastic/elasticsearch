/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * Synced flush ActionType.
 */
public class TransportSyncedFlushAction extends HandledTransportAction<SyncedFlushRequest, SyncedFlushResponse> {

    SyncedFlushService syncedFlushService;

    @Inject
    public TransportSyncedFlushAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SyncedFlushService syncedFlushService
    ) {
        super(SyncedFlushAction.NAME, transportService, actionFilters, SyncedFlushRequest::new);
        this.syncedFlushService = syncedFlushService;
    }

    @Override
    protected void doExecute(Task task, SyncedFlushRequest request, ActionListener<SyncedFlushResponse> listener) {
        syncedFlushService.attemptSyncedFlush(request.indices(), request.indicesOptions(), listener);
    }
}
