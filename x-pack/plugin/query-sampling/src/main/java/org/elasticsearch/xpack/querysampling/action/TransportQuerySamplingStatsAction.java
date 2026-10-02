/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.querysampling.QuerySamplingService;

import java.io.IOException;
import java.util.List;

/**
 * Collects the query sampling stats of every selected node. Sampling state is kept per coordinating
 * node, so this is also the place where a cluster-wide view is assembled.
 */
public final class TransportQuerySamplingStatsAction extends TransportNodesAction<
    QuerySamplingStatsRequest,
    QuerySamplingStatsResponse,
    TransportQuerySamplingStatsAction.NodeRequest,
    QuerySamplingNodeStatsResponse,
    Void> {

    private final QuerySamplingService querySamplingService;

    @Inject
    public TransportQuerySamplingStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        QuerySamplingService querySamplingService
    ) {
        super(
            QuerySamplingStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.querySamplingService = querySamplingService;
    }

    @Override
    protected QuerySamplingStatsResponse newResponse(
        QuerySamplingStatsRequest request,
        List<QuerySamplingNodeStatsResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new QuerySamplingStatsResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(QuerySamplingStatsRequest request) {
        return new NodeRequest();
    }

    @Override
    protected QuerySamplingNodeStatsResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new QuerySamplingNodeStatsResponse(in);
    }

    @Override
    protected QuerySamplingNodeStatsResponse nodeOperation(NodeRequest request, Task task) {
        return new QuerySamplingNodeStatsResponse(clusterService.localNode(), querySamplingService.stats());
    }

    static final class NodeRequest extends AbstractTransportRequest {
        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
