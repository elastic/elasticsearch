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
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;

public class TransportFinalizeJobExecutionAction extends TransportMasterNodeAction<FinalizeJobExecutionAction.Request,
    AcknowledgedResponse> {

    @Inject
    public TransportFinalizeJobExecutionAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, FinalizeJobExecutionAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, FinalizeJobExecutionAction.Request::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(FinalizeJobExecutionAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        // This action is no longer required but needs to be preserved
        // in case it is called by an old node in a mixed cluster
        listener.onResponse(new AcknowledgedResponse(true));
    }

    @Override
    protected ClusterBlockException checkBlock(FinalizeJobExecutionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
