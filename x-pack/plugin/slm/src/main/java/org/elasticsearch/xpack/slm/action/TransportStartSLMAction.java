/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.OperationModeUpdateTask;
import org.elasticsearch.xpack.core.slm.action.StartSLMAction;

public class TransportStartSLMAction extends AcknowledgedTransportMasterNodeAction<StartSLMAction.Request> {

    @Inject
    public TransportStartSLMAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            StartSLMAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartSLMAction.Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        StartSLMAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask(
            "slm_operation_mode_update[running]",
            OperationModeUpdateTask.wrap(OperationModeUpdateTask.slmMode(OperationMode.RUNNING), request, listener)
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(StartSLMAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
