/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.TransportVersions.V_8_11_X;

public class TransportNodesInfoAction extends TransportNodesAction<
    NodesInfoRequest,
    NodesInfoResponse,
    TransportNodesInfoAction.NodeInfoRequest,
    NodeInfo> {

    public static final ActionType<NodesInfoResponse> TYPE = new ActionType<>("cluster:monitor/nodes/info");
    private final NodeService nodeService;

    @Inject
    public TransportNodesInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeInfoRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.nodeService = nodeService;
    }

    @Override
    protected NodesInfoResponse newResponse(
        NodesInfoRequest nodesInfoRequest,
        List<NodeInfo> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesInfoResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeInfoRequest newNodeRequest(NodesInfoRequest request) {
        return new NodeInfoRequest(request);
    }

    @Override
    protected NodeInfo newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeInfo(in);
    }

    @Override
    protected NodeInfo nodeOperation(NodeInfoRequest nodeRequest, Task task) {
        Set<String> metrics = nodeRequest.requestedMetrics();
        return nodeService.info(
            metrics.contains(NodesInfoMetrics.Metric.SETTINGS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.OS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.PROCESS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.JVM.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.THREAD_POOL.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.TRANSPORT.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.HTTP.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.REMOTE_CLUSTER_SERVER.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.PLUGINS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.INGEST.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.AGGREGATIONS.metricName()),
            metrics.contains(NodesInfoMetrics.Metric.INDICES.metricName())
        );
    }

    public static class NodeInfoRequest extends TransportRequest {

        private final NodesInfoMetrics nodesInfoMetrics;

        public NodeInfoRequest(StreamInput in) throws IOException {
            super(in);
            skipLegacyNodesRequestHeader(V_8_11_X, in);
            this.nodesInfoMetrics = new NodesInfoMetrics(in);
        }

        public NodeInfoRequest(NodesInfoRequest request) {
            this.nodesInfoMetrics = request.getNodesInfoMetrics();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sendLegacyNodesRequestHeader(V_8_11_X, out);
            nodesInfoMetrics.writeTo(out);
        }

        public Set<String> requestedMetrics() {
            return nodesInfoMetrics.requestedMetrics();
        }
    }
}
