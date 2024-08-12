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
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

public class ClearCcrRestoreSessionAction extends ActionType<ActionResponse.Empty> {

    public static final ClearCcrRestoreSessionAction INTERNAL_INSTANCE = new ClearCcrRestoreSessionAction();
    public static final String INTERNAL_NAME = "internal:admin/ccr/restore/session/clear";
    public static final String NAME = "indices:internal/admin/ccr/restore/session/clear";
    public static final ClearCcrRestoreSessionAction INSTANCE = new ClearCcrRestoreSessionAction(NAME);

    public static final RemoteClusterActionType<ActionResponse.Empty> REMOTE_TYPE = RemoteClusterActionType.emptyResponse(NAME);
    public static final RemoteClusterActionType<ActionResponse.Empty> REMOTE_INTERNAL_TYPE = RemoteClusterActionType.emptyResponse(
        INTERNAL_NAME
    );

    private ClearCcrRestoreSessionAction() {
        this(INTERNAL_NAME);
    }

    private ClearCcrRestoreSessionAction(String name) {
        super(name);
    }

    abstract static class TransportDeleteCcrRestoreSessionAction extends HandledTransportAction<
        ClearCcrRestoreSessionRequest,
        ActionResponse.Empty> {

        protected final CcrRestoreSourceService ccrRestoreService;

        private TransportDeleteCcrRestoreSessionAction(
            String actionName,
            ActionFilters actionFilters,
            TransportService transportService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(
                actionName,
                transportService,
                actionFilters,
                ClearCcrRestoreSessionRequest::new,
                transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
            );
            TransportActionProxy.registerProxyAction(transportService, actionName, false, in -> ActionResponse.Empty.INSTANCE);
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected void doExecute(Task task, ClearCcrRestoreSessionRequest request, ActionListener<ActionResponse.Empty> listener) {
            validate(request);
            ccrRestoreService.closeSession(request.getSessionUUID());
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }

        // We don't enforce any validation by default so that the internal action stays the same for BWC reasons
        protected void validate(ClearCcrRestoreSessionRequest request) {}
    }

    public static class InternalTransportAction extends TransportDeleteCcrRestoreSessionAction {
        @Inject
        public InternalTransportAction(
            ActionFilters actionFilters,
            TransportService transportService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(INTERNAL_NAME, actionFilters, transportService, ccrRestoreService);
        }
    }

    public static class TransportAction extends TransportDeleteCcrRestoreSessionAction {
        @Inject
        public TransportAction(ActionFilters actionFilters, TransportService transportService, CcrRestoreSourceService ccrRestoreService) {
            super(NAME, actionFilters, transportService, ccrRestoreService);
        }

        @Override
        protected void validate(ClearCcrRestoreSessionRequest request) {
            final ShardId shardId = request.getShardId();
            assert shardId != null : "shardId must be specified for the request";
            ccrRestoreService.ensureSessionShardIdConsistency(request.getSessionUUID(), shardId);
        }
    }
}
