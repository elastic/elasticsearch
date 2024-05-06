/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceStatsAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportGetInferenceStatsAction extends TransportNodesAction<
    GetInferenceStatsAction.Request,
    GetInferenceStatsAction.Response,
    GetInferenceStatsAction.NodeRequest,
    GetInferenceStatsAction.NodeResponse> {

    private final HttpClientManager httpClientManager;

    @Inject
    public TransportGetInferenceStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        HttpClientManager httpClientManager
    ) {
        super(
            GetInferenceStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetInferenceStatsAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );

        this.httpClientManager = Objects.requireNonNull(httpClientManager);
    }

    @Override
    protected GetInferenceStatsAction.Response newResponse(
        GetInferenceStatsAction.Request request,
        List<GetInferenceStatsAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetInferenceStatsAction.Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetInferenceStatsAction.NodeRequest newNodeRequest(GetInferenceStatsAction.Request request) {
        return new GetInferenceStatsAction.NodeRequest();
    }

    @Override
    protected GetInferenceStatsAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetInferenceStatsAction.NodeResponse(in);
    }

    @Override
    protected GetInferenceStatsAction.NodeResponse nodeOperation(GetInferenceStatsAction.NodeRequest request, Task task) {
        return new GetInferenceStatsAction.NodeResponse(transportService.getLocalNode(), httpClientManager.getPoolStats());
    }
}
