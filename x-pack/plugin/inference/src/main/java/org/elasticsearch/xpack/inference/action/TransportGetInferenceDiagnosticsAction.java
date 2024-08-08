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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportGetInferenceDiagnosticsAction extends TransportNodesAction<
    GetInferenceDiagnosticsAction.Request,
    GetInferenceDiagnosticsAction.Response,
    GetInferenceDiagnosticsAction.NodeRequest,
    GetInferenceDiagnosticsAction.NodeResponse> {

    private final HttpClientManager httpClientManager;

    @Inject
    public TransportGetInferenceDiagnosticsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        HttpClientManager httpClientManager
    ) {
        super(
            GetInferenceDiagnosticsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetInferenceDiagnosticsAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );

        this.httpClientManager = Objects.requireNonNull(httpClientManager);
    }

    @Override
    protected GetInferenceDiagnosticsAction.Response newResponse(
        GetInferenceDiagnosticsAction.Request request,
        List<GetInferenceDiagnosticsAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetInferenceDiagnosticsAction.Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeRequest newNodeRequest(GetInferenceDiagnosticsAction.Request request) {
        return new GetInferenceDiagnosticsAction.NodeRequest();
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetInferenceDiagnosticsAction.NodeResponse(in);
    }

    @Override
    protected GetInferenceDiagnosticsAction.NodeResponse nodeOperation(GetInferenceDiagnosticsAction.NodeRequest request, Task task) {
        return new GetInferenceDiagnosticsAction.NodeResponse(transportService.getLocalNode(), httpClientManager.getPoolStats());
    }
}
