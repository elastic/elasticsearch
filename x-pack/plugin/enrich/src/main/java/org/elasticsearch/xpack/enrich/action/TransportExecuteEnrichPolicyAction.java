/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPolicyExecutor;
import org.elasticsearch.xpack.enrich.PolicyExecutionResult;

import java.io.IOException;

public class TransportExecuteEnrichPolicyAction
    extends TransportMasterNodeReadAction<ExecuteEnrichPolicyAction.Request, AcknowledgedResponse> {

    private final EnrichPolicyExecutor executor;

    @Inject
    public TransportExecuteEnrichPolicyAction(TransportService transportService,
                                              ClusterService clusterService,
                                              ThreadPool threadPool,
                                              ActionFilters actionFilters,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              EnrichPolicyExecutor executor) {
        super(ExecuteEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ExecuteEnrichPolicyAction.Request::new, indexNameExpressionResolver);
        this.executor = executor;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, ExecuteEnrichPolicyAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        executor.runPolicy(request.getName(), new ActionListener<>() {
            @Override
            public void onResponse(PolicyExecutionResult policyExecutionResult) {
                listener.onResponse(new AcknowledgedResponse(policyExecutionResult.isCompleted()));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
