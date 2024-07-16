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
import org.elasticsearch.xpack.core.inference.action.GetInternalInferenceUsageAction;
import org.elasticsearch.xpack.inference.telemetry.InferenceRequestStatsMap;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportGetInternalInferenceUsageAction extends TransportNodesAction<
    GetInternalInferenceUsageAction.Request,
    GetInternalInferenceUsageAction.Response,
    GetInternalInferenceUsageAction.NodeRequest,
    GetInternalInferenceUsageAction.NodeResponse> {

    private final InferenceRequestStatsMap statsMap;

    @Inject
    public TransportGetInternalInferenceUsageAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        InferenceRequestStatsMap statsMap
    ) {
        super(
            GetInternalInferenceUsageAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetInternalInferenceUsageAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );

        this.statsMap = Objects.requireNonNull(statsMap);
    }

    @Override
    protected GetInternalInferenceUsageAction.Response newResponse(
        GetInternalInferenceUsageAction.Request request,
        List<GetInternalInferenceUsageAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetInternalInferenceUsageAction.Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeRequest newNodeRequest(GetInternalInferenceUsageAction.Request request) {
        return new GetInternalInferenceUsageAction.NodeRequest();
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetInternalInferenceUsageAction.NodeResponse(in);
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeResponse nodeOperation(GetInternalInferenceUsageAction.NodeRequest request, Task task) {
        return new GetInternalInferenceUsageAction.NodeResponse(transportService.getLocalNode(), statsMap.toSerializableMap());
    }
}
