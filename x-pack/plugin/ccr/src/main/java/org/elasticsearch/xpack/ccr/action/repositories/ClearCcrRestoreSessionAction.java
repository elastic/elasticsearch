/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class ClearCcrRestoreSessionAction extends ActionType<ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse> {

    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction();
    public static final String NAME = "internal:admin/ccr/restore/session/clear";

    private ClearCcrRestoreSessionAction() {
        super(NAME, ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse::new);
    }

    public static class TransportDeleteCcrRestoreSessionAction
        extends HandledTransportAction<ClearCcrRestoreSessionRequest, ClearCcrRestoreSessionResponse> {

        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(ActionFilters actionFilters, TransportService transportService,
                                                      CcrRestoreSourceService ccrRestoreService) {
            super(NAME, transportService, actionFilters, ClearCcrRestoreSessionRequest::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, ClearCcrRestoreSessionResponse::new);
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected void doExecute(Task task, ClearCcrRestoreSessionRequest request,
                                 ActionListener<ClearCcrRestoreSessionResponse> listener) {
            ccrRestoreService.closeSession(request.getSessionUUID());
            listener.onResponse(new ClearCcrRestoreSessionResponse());
        }
    }

    public static class ClearCcrRestoreSessionResponse extends ActionResponse {

        ClearCcrRestoreSessionResponse() {
        }

        ClearCcrRestoreSessionResponse(StreamInput in) {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
