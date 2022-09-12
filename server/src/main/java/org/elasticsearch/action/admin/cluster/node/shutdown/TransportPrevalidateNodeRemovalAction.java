/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

public class TransportPrevalidateNodeRemovalAction extends TransportMasterNodeReadAction<
    PrevalidateNodeRemovalRequest,
    PrevalidateNodeRemovalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPrevalidateNodeRemovalAction.class);

    @Inject
    public TransportPrevalidateNodeRemovalAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PrevalidateNodeRemovalAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PrevalidateNodeRemovalRequest::new,
            indexNameExpressionResolver,
            PrevalidateNodeRemovalResponse::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        PrevalidateNodeRemovalRequest request,
        ClusterState state,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) throws Exception {
        // TODO: Need to set masterNodeTimeOut
        doPrevalidation(request, new ClusterStateHealth(state), listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PrevalidateNodeRemovalRequest request, ClusterState state) {
        // Allow running this action even when there are blocks on the cluster
        return null;
    }

    private void doPrevalidation(
        PrevalidateNodeRemovalRequest prevalidationRequest,
        ClusterStateHealth clusterStateHealth,
        ActionListener<PrevalidateNodeRemovalResponse> listener
    ) {
        switch (clusterStateHealth.getStatus()) {
            case GREEN, YELLOW -> {
                Result overall = new Result(IsSafe.YES, "");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.YES, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
            case RED -> {
                // TODO: search for RED indices which are searchable snapshot based.
                Result overall = new Result(IsSafe.UNKNOWN, "cluster health is RED");
                Map<String, Result> nodeResults = prevalidationRequest.getNodeIds()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), id -> new Result(IsSafe.UNKNOWN, "")));
                listener.onResponse(new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(overall, nodeResults)));
            }
        }
    }
}
