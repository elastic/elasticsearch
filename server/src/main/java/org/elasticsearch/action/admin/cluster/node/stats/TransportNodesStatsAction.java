/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportNodesStatsAction extends TransportNodesAction<
    NodesStatsRequest,
    NodesStatsResponse,
    TransportNodesStatsAction.NodeStatsRequest,
    NodeStats> {

    private final NodeService nodeService;

    @Inject
    public TransportNodesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters
    ) {
        super(
            NodesStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesStatsRequest::new,
            NodeStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeStats.class
        );
        this.nodeService = nodeService;
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest request, List<NodeStats> responses, List<FailedNodeException> failures) {
        return new NodesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStatsRequest newNodeRequest(NodesStatsRequest request) {
        return new NodeStatsRequest(request);
    }

    @Override
    protected NodeStats newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeStats(in);
    }

    @Override
    protected NodeStats nodeOperation(NodeStatsRequest nodeStatsRequest, Task task) {
        assert task instanceof CancellableTask;

        NodesStatsRequest request = nodeStatsRequest.request;
        Set<String> metrics = request.requestedMetrics();
        return nodeService.stats(
            request.indices(),
            NodesStatsRequest.Metric.OS.containedIn(metrics),
            NodesStatsRequest.Metric.PROCESS.containedIn(metrics),
            NodesStatsRequest.Metric.JVM.containedIn(metrics),
            NodesStatsRequest.Metric.THREAD_POOL.containedIn(metrics),
            NodesStatsRequest.Metric.FS.containedIn(metrics),
            NodesStatsRequest.Metric.TRANSPORT.containedIn(metrics),
            NodesStatsRequest.Metric.HTTP.containedIn(metrics),
            NodesStatsRequest.Metric.BREAKER.containedIn(metrics),
            NodesStatsRequest.Metric.SCRIPT.containedIn(metrics),
            NodesStatsRequest.Metric.DISCOVERY.containedIn(metrics),
            NodesStatsRequest.Metric.INGEST.containedIn(metrics),
            NodesStatsRequest.Metric.ADAPTIVE_SELECTION.containedIn(metrics),
            NodesStatsRequest.Metric.SCRIPT_CACHE.containedIn(metrics),
            NodesStatsRequest.Metric.INDEXING_PRESSURE.containedIn(metrics),
            NodesStatsRequest.Metric.REPOSITORY.containedIn(metrics)
        );
    }

    public static class NodeStatsRequest extends TransportRequest {

        NodesStatsRequest request;

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesStatsRequest(in);
        }

        NodeStatsRequest(NodesStatsRequest request) {
            this.request = request;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
