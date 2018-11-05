/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.RefreshJobMemoryRequirementAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

public class TransportRefreshJobMemoryRequirementAction
    extends TransportMasterNodeAction<RefreshJobMemoryRequirementAction.Request, AcknowledgedResponse> {

    private final MlMemoryTracker memoryTracker;

    @Inject
    public TransportRefreshJobMemoryRequirementAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                      ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                                      MlMemoryTracker memoryTracker) {
        super(settings, RefreshJobMemoryRequirementAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, RefreshJobMemoryRequirementAction.Request::new);
        this.memoryTracker = memoryTracker;
    }

    @Override
    protected String executor() {
        return MachineLearning.UTILITY_THREAD_POOL_NAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(RefreshJobMemoryRequirementAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        try {
            memoryTracker.refreshJobMemory(request.getJobId(), mem -> listener.onResponse(new AcknowledgedResponse(mem != null)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(RefreshJobMemoryRequirementAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }
}
