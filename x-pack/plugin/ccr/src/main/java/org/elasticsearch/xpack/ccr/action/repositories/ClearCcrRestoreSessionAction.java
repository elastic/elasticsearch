/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

public class ClearCcrRestoreSessionAction extends Action<ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse> {

    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/clear";

    private ClearCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public ClearCcrRestoreSessionResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<ClearCcrRestoreSessionResponse> getResponseReader() {
        return ClearCcrRestoreSessionResponse::new;
    }

    public static class TransportDeleteCcrRestoreSessionAction
        extends HandledTransportAction<ClearCcrRestoreSessionRequest, ClearCcrRestoreSessionResponse> {

        private final CcrRestoreSourceService ccrRestoreService;
        private final ThreadPool threadPool;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(ActionFilters actionFilters, TransportService transportService,
                                                      CcrRestoreSourceService ccrRestoreService) {
            super(NAME, transportService, actionFilters, ClearCcrRestoreSessionRequest::new);
            TransportActionProxy.registerProxyAction(transportService, NAME, ClearCcrRestoreSessionResponse::new);
            this.ccrRestoreService = ccrRestoreService;
            this.threadPool = transportService.getThreadPool();
        }

        @Override
        protected void doExecute(Task task, ClearCcrRestoreSessionRequest request,
                                 ActionListener<ClearCcrRestoreSessionResponse> listener) {
            // TODO: Currently blocking actions might occur in the session closed callbacks. This dispatch
            //  may be unnecessary when we remove these callbacks.
            threadPool.generic().execute(() ->  {
                ccrRestoreService.closeSession(request.getSessionUUID());
                listener.onResponse(new ClearCcrRestoreSessionResponse());
            });
        }
    }

    public static class ClearCcrRestoreSessionResponse extends ActionResponse {

        ClearCcrRestoreSessionResponse() {
        }

        ClearCcrRestoreSessionResponse(StreamInput in) {
        }
    }
}
