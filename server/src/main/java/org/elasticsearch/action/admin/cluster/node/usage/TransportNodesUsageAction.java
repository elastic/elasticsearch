/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransportNodesUsageAction extends TransportNodesAction<
    NodesUsageRequest,
    NodesUsageResponse,
    TransportNodesUsageAction.NodeUsageRequest,
    NodeUsage,
    Void> {

    public static final ActionType<NodesUsageResponse> TYPE = new ActionType<>("cluster:monitor/nodes/usage");
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
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeUsageRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
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
    protected NodeUsage nodeOperation(NodeUsageRequest request, Task task) {
        Map<String, Long> restUsage = request.restActions ? restUsageService.getRestUsageStats() : null;
        Map<String, Object> aggsUsage = request.aggregations ? aggregationUsageService.getUsageStats() : null;
        return new NodeUsage(clusterService.localNode(), System.currentTimeMillis(), sinceTime, restUsage, aggsUsage);
    }

    public static class NodeUsageRequest extends AbstractTransportRequest {

        final boolean restActions;
        final boolean aggregations;

        public NodeUsageRequest(StreamInput in) throws IOException {
            super(in);
            restActions = in.readBoolean();
            aggregations = in.readBoolean();
        }

        NodeUsageRequest(NodesUsageRequest request) {
            restActions = request.restActions();
            aggregations = request.aggregations();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(restActions);
            out.writeBoolean(aggregations);
        }
    }
}
