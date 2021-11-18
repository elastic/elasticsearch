/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

public class ClearCcrRestoreSessionAction extends ActionType<ActionResponse.Empty> {

    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction();
    public static final String NAME = "internal:admin/ccr/restore/session/clear";

    private ClearCcrRestoreSessionAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class TransportDeleteCcrRestoreSessionAction extends HandledTransportAction<
        ClearCcrRestoreSessionRequest,
        ActionResponse.Empty> {

        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(
            ActionFilters actionFilters,
            TransportService transportService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(NAME, transportService, actionFilters, ClearCcrRestoreSessionRequest::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, false, in -> ActionResponse.Empty.INSTANCE);
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected void doExecute(Task task, ClearCcrRestoreSessionRequest request, ActionListener<ActionResponse.Empty> listener) {
            ccrRestoreService.closeSession(request.getSessionUUID());
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }
    }
}
