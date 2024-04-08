/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetAllocationStatsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.TransportVersions.NODE_STATS_REQUEST_SIMPLIFIED;

public class TransportNodesStatsAction extends TransportNodesAction<
    NodesStatsRequest,
    NodesStatsResponse,
    TransportNodesStatsAction.NodeStatsRequest,
    NodeStats> {

    public static final ActionType<NodesStatsResponse> TYPE = new ActionType<>("cluster:monitor/nodes/stats");

    private final NodeService nodeService;
    private final NodeClient client;

    @Inject
    public TransportNodesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        ActionFilters actionFilters,
        NodeClient client
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeStatsRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.nodeService = nodeService;
        this.client = client;
    }

    @Override
    protected NodesStatsResponse newResponse(NodesStatsRequest request, List<NodeStats> responses, List<FailedNodeException> failures) {
        return new NodesStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected void newResponseAsync(
        Task task,
        NodesStatsRequest request,
        List<NodeStats> responses,
        List<FailedNodeException> failures,
        ActionListener<NodesStatsResponse> listener
    ) {
        Set<String> metrics = request.getNodesStatsRequestParameters().requestedMetrics();
        if (NodesStatsRequestParameters.Metric.ALLOCATIONS.containedIn(metrics)) {
            client.execute(
                TransportGetAllocationStatsAction.TYPE,
                new TransportGetAllocationStatsAction.Request(new TaskId(clusterService.localNode().getId(), task.getId())),
                listener.delegateFailure((l, r) -> {
                    ActionListener.respondAndRelease(l, newResponse(request, merge(responses, r.getNodeAllocationStats()), failures));
                })
            );
        } else {
            ActionListener.run(listener, l -> ActionListener.respondAndRelease(l, newResponse(request, responses, failures)));
        }
    }

    private static List<NodeStats> merge(List<NodeStats> responses, Map<String, NodeAllocationStats> allocationStats) {
        return responses.stream()
            .map(response -> response.withNodeAllocationStats(allocationStats.get(response.getNode().getId())))
            .toList();
    }

    @Override
    protected NodeStatsRequest newNodeRequest(NodesStatsRequest request) {
        return new NodeStatsRequest(request);
    }

    @Override
    protected NodeStats newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        assert Transports.assertNotTransportThread("deserializing node stats is too expensive for a transport thread");
        return new NodeStats(in);
    }

    @Override
    protected NodeStats nodeOperation(NodeStatsRequest request, Task task) {
        assert task instanceof CancellableTask;

        final NodesStatsRequestParameters nodesStatsRequestParameters = request.getNodesStatsRequestParameters();
        Set<String> metrics = nodesStatsRequestParameters.requestedMetrics();
        return nodeService.stats(
            nodesStatsRequestParameters.indices(),
            nodesStatsRequestParameters.includeShardsStats(),
            NodesStatsRequestParameters.Metric.OS.containedIn(metrics),
            NodesStatsRequestParameters.Metric.PROCESS.containedIn(metrics),
            NodesStatsRequestParameters.Metric.JVM.containedIn(metrics),
            NodesStatsRequestParameters.Metric.THREAD_POOL.containedIn(metrics),
            NodesStatsRequestParameters.Metric.FS.containedIn(metrics),
            NodesStatsRequestParameters.Metric.TRANSPORT.containedIn(metrics),
            NodesStatsRequestParameters.Metric.HTTP.containedIn(metrics),
            NodesStatsRequestParameters.Metric.BREAKER.containedIn(metrics),
            NodesStatsRequestParameters.Metric.SCRIPT.containedIn(metrics),
            NodesStatsRequestParameters.Metric.DISCOVERY.containedIn(metrics),
            NodesStatsRequestParameters.Metric.INGEST.containedIn(metrics),
            NodesStatsRequestParameters.Metric.ADAPTIVE_SELECTION.containedIn(metrics),
            NodesStatsRequestParameters.Metric.SCRIPT_CACHE.containedIn(metrics),
            NodesStatsRequestParameters.Metric.INDEXING_PRESSURE.containedIn(metrics),
            NodesStatsRequestParameters.Metric.REPOSITORIES.containedIn(metrics)
        );
    }

    public static class NodeStatsRequest extends TransportRequest {

        private NodesStatsRequestParameters nodesStatsRequestParameters;
        private String[] nodesIds;

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(NODE_STATS_REQUEST_SIMPLIFIED)) {
                this.nodesStatsRequestParameters = new NodesStatsRequestParameters(in);
                this.nodesIds = in.readStringArray();
            } else {
                final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(in);
                this.nodesStatsRequestParameters = nodesStatsRequest.getNodesStatsRequestParameters();
                this.nodesIds = nodesStatsRequest.nodesIds();
            }
        }

        NodeStatsRequest(NodesStatsRequest request) {
            this.nodesStatsRequestParameters = request.getNodesStatsRequestParameters();
            this.nodesIds = request.nodesIds();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return Strings.format(
                        "nodes=%s, metrics=%s, flags=%s",
                        Arrays.toString(nodesIds),
                        nodesStatsRequestParameters.requestedMetrics().toString(),
                        Arrays.toString(nodesStatsRequestParameters.indices().getFlags())
                    );
                }
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(NODE_STATS_REQUEST_SIMPLIFIED)) {
                this.nodesStatsRequestParameters.writeTo(out);
                out.writeStringArrayNullable(nodesIds);
            } else {
                new NodesStatsRequest(nodesStatsRequestParameters, this.nodesIds).writeTo(out);
            }
        }

        public NodesStatsRequestParameters getNodesStatsRequestParameters() {
            return nodesStatsRequestParameters;
        }
    }
}
