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
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class ClearCcrRestoreSessionAction extends Action<ClearCcrRestoreSessionRequest,
    ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse, ClearCcrRestoreSessionRequestBuilder> {

    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction();
    public static final String NAME = "internal:admin/ccr/restore/session/clear";

    private ClearCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public ClearCcrRestoreSessionResponse newResponse() {
        return new ClearCcrRestoreSessionResponse();
    }

    @Override
    public ClearCcrRestoreSessionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ClearCcrRestoreSessionRequestBuilder(client);
    }

    public static class TransportDeleteCcrRestoreSessionAction
        extends HandledTransportAction<ClearCcrRestoreSessionRequest, ClearCcrRestoreSessionResponse> {

        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver resolver,
                                                      TransportService transportService, CcrRestoreSourceService ccrRestoreService) {
            super(settings, NAME, threadPool, transportService, actionFilters, resolver, ClearCcrRestoreSessionRequest::new,
                ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, ClearCcrRestoreSessionResponse::new);
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected void doExecute(ClearCcrRestoreSessionRequest request, ActionListener<ClearCcrRestoreSessionResponse> listener) {
            ccrRestoreService.closeSession(request.getSessionUUID());
            listener.onResponse(new ClearCcrRestoreSessionResponse());
        }
    }

    public static class ClearCcrRestoreSessionResponse extends ActionResponse {

        ClearCcrRestoreSessionResponse() {
        }

        ClearCcrRestoreSessionResponse(StreamInput in) throws IOException {
            super(in);
        }
    }
}
