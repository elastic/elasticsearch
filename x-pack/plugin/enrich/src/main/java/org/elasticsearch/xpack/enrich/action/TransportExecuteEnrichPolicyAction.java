/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;

/**
 * Coordinates enrich policy executions. This is a master node action,
 * so that policy executions can be accounted. For example that no more
 * than X policy executions occur or only a single policy execution occurs
 * for each policy. The actual execution of the enrich policy is performed
 * via {@link InternalExecutePolicyAction}.
 */
public class TransportExecuteEnrichPolicyAction extends TransportMasterNodeAction<
    ExecuteEnrichPolicyAction.Request,
    ExecuteEnrichPolicyAction.Response> {

    private final EnrichPolicyExecutor executor;

    @Inject
    public TransportExecuteEnrichPolicyAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        EnrichPolicyExecutor enrichPolicyExecutor
    ) {
        super(
            ExecuteEnrichPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExecuteEnrichPolicyAction.Request::new,
            indexNameExpressionResolver,
            ExecuteEnrichPolicyAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.executor = enrichPolicyExecutor;
    }

    @Override
    protected void masterOperation(
        Task task,
        ExecuteEnrichPolicyAction.Request request,
        ClusterState state,
        ActionListener<ExecuteEnrichPolicyAction.Response> listener
    ) {
        executor.coordinatePolicyExecution(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
