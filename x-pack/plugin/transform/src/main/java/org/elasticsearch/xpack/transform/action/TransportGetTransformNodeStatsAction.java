/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

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
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodeStatsRequest;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodeStatsResponse;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodesStatsRequest;
import org.elasticsearch.xpack.core.transform.action.GetTransformNodeStatsAction.NodesStatsResponse;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.io.IOException;
import java.util.List;

/**
 * {@link TransportGetTransformNodeStatsAction} class fetches transform-related metrics from all the nodes and aggregates these metrics.
 */
public class TransportGetTransformNodeStatsAction extends TransportNodesAction<
    NodesStatsRequest,
    NodesStatsResponse,
    NodeStatsRequest,
    NodeStatsResponse> {

    private final TransportService transportService;
    private final TransformScheduler scheduler;

    @Inject
    public TransportGetTransformNodeStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        TransformServices transformServices
    ) {
        super(
            GetTransformNodeStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeStatsRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.transportService = transportService;
        this.scheduler = transformServices.scheduler();
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest request, List<NodeStatsResponse> nodes, List<FailedNodeException> failures) {
        return new NodesStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(NodesStatsRequest request) {
        return new NodeStatsRequest();
    }

    @Override
    protected NodeStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeStatsResponse(in);
    }

    @Override
    protected NodeStatsResponse nodeOperation(NodeStatsRequest request, Task task) {
        var localNode = transportService.getLocalNode();
        var schedulerStats = scheduler.getStats();
        return new NodeStatsResponse(localNode, schedulerStats);
    }
}
