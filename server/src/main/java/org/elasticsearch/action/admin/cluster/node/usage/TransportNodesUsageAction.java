/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransportNodesUsageAction extends TransportNodesAction<
    NodesUsageRequest,
    NodesUsageResponse,
    TransportNodesUsageAction.NodeUsageRequest,
    NodeUsage> {

    private final UsageService restUsageService;
    private final AggregationUsageService aggregationUsageService;
    private final long sinceTime;

    @Inject
    public TransportNodesUsageAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        UsageService restUsageService,
        AggregationUsageService aggregationUsageService
    ) {
        super(
            NodesUsageAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesUsageRequest::new,
            NodeUsageRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeUsage.class
        );
        this.restUsageService = restUsageService;
        this.aggregationUsageService = aggregationUsageService;
        this.sinceTime = System.currentTimeMillis();
    }

    @Override
    protected NodesUsageResponse newResponse(NodesUsageRequest request, List<NodeUsage> responses, List<FailedNodeException> failures) {
        return new NodesUsageResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeUsageRequest newNodeRequest(NodesUsageRequest request) {
        return new NodeUsageRequest(request);
    }

    @Override
    protected NodeUsage newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeUsage(in);
    }

    @Override
    protected NodeUsage nodeOperation(NodeUsageRequest nodeUsageRequest, Task task) {
        NodesUsageRequest request = nodeUsageRequest.request;
        Map<String, Long> restUsage = request.restActions() ? restUsageService.getRestUsageStats() : null;
        Map<String, Object> aggsUsage = request.aggregations() ? aggregationUsageService.getUsageStats() : null;
        return new NodeUsage(clusterService.localNode(), System.currentTimeMillis(), sinceTime, restUsage, aggsUsage);
    }

    public static class NodeUsageRequest extends TransportRequest {

        NodesUsageRequest request;

        public NodeUsageRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesUsageRequest(in);
        }

        NodeUsageRequest(NodesUsageRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
