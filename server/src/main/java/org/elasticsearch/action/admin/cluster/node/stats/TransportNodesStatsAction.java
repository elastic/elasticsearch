/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetAllocationStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.rest.RestUtils;
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
import java.util.Objects;

public class TransportNodesStatsAction extends TransportNodesAction<
    NodesStatsRequest,
    NodesStatsResponse,
    TransportNodesStatsAction.NodeStatsRequest,
    NodeStats,
    SubscribableListener<TransportGetAllocationStatsAction.Response>> {

    public static final ActionType<NodesStatsResponse> TYPE = new ActionType<>("cluster:monitor/nodes/stats");

    private final NodeService nodeService;
    private final NodeClient client;

    @Inject
    public TransportNodesStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeService nodeService,
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
        assert false;
        throw new UnsupportedOperationException("use newResponseAsync instead");
    }

    @Override
    protected SubscribableListener<TransportGetAllocationStatsAction.Response> createActionContext(Task task, NodesStatsRequest request) {
        return SubscribableListener.newForked(l -> {
            var metrics = request.getNodesStatsRequestParameters().requestedMetrics();
            if (metrics.contains(Metric.FS) || metrics.contains(Metric.ALLOCATIONS)) {
                new ParentTaskAssigningClient(client, clusterService.localNode(), task).execute(
                    TransportGetAllocationStatsAction.TYPE,
                    new TransportGetAllocationStatsAction.Request(
                        Objects.requireNonNullElse(request.timeout(), RestUtils.REST_MASTER_TIMEOUT_DEFAULT),
                        new TaskId(clusterService.localNode().getId(), task.getId()),
                        metrics
                    ),
                    l
                );
            } else {
                l.onResponse(null);
            }
        });
    }

    @Override
    protected void newResponseAsync(
        Task task,
        NodesStatsRequest request,
        SubscribableListener<TransportGetAllocationStatsAction.Response> actionContext,
        List<NodeStats> responses,
        List<FailedNodeException> failures,
        ActionListener<NodesStatsResponse> listener
    ) {
        actionContext
            // merge in the stats from the master, if available
            .andThenApply(
                getAllocationStatsResponse -> new NodesStatsResponse(
                    clusterService.getClusterName(),
                    getAllocationStatsResponse == null
                        ? responses
                        : merge(
                            responses,
                            getAllocationStatsResponse.getNodeAllocationStats(),
                            getAllocationStatsResponse.getDiskThresholdSettings()
                        ),
                    failures
                )
            )
            .addListener(listener);
    }

    private static List<NodeStats> merge(
        List<NodeStats> responses,
        Map<String, NodeAllocationStats> allocationStats,
        DiskThresholdSettings masterThresholdSettings
    ) {
        return responses.stream()
            .map(response -> response.withNodeAllocationStats(allocationStats.get(response.getNode().getId()), masterThresholdSettings))
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

        final var nodesStatsRequestParameters = request.getNodesStatsRequestParameters();
        final var metrics = nodesStatsRequestParameters.requestedMetrics();

        return nodeService.stats(
            nodesStatsRequestParameters.indices(),
            nodesStatsRequestParameters.includeShardsStats(),
            metrics.contains(Metric.OS),
            metrics.contains(Metric.PROCESS),
            metrics.contains(Metric.JVM),
            metrics.contains(Metric.THREAD_POOL),
            metrics.contains(Metric.FS),
            metrics.contains(Metric.TRANSPORT),
            metrics.contains(Metric.HTTP),
            metrics.contains(Metric.BREAKER),
            metrics.contains(Metric.SCRIPT),
            metrics.contains(Metric.DISCOVERY),
            metrics.contains(Metric.INGEST),
            metrics.contains(Metric.ADAPTIVE_SELECTION),
            metrics.contains(Metric.SCRIPT_CACHE),
            metrics.contains(Metric.INDEXING_PRESSURE),
            metrics.contains(Metric.REPOSITORIES)
        );
    }

    public static class NodeStatsRequest extends TransportRequest {

        private final NodesStatsRequestParameters nodesStatsRequestParameters;

        public NodeStatsRequest(StreamInput in) throws IOException {
            super(in);
            this.nodesStatsRequestParameters = new NodesStatsRequestParameters(in);
            if (in.getTransportVersion().between(TransportVersions.V_8_13_0, TransportVersions.V_8_15_0)) {
                in.readStringArray(); // formerly nodeIds, now unused
            }
        }

        NodeStatsRequest(NodesStatsRequest request) {
            this.nodesStatsRequestParameters = request.getNodesStatsRequestParameters();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return Strings.format(
                        "metrics=%s, flags=%s",
                        nodesStatsRequestParameters.requestedMetrics().toString(),
                        Arrays.toString(nodesStatsRequestParameters.indices().getFlags())
                    );
                }
            };
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            nodesStatsRequestParameters.writeTo(out);
            if (out.getTransportVersion().between(TransportVersions.V_8_13_0, TransportVersions.V_8_15_0)) {
                out.writeStringArray(Strings.EMPTY_ARRAY); // formerly nodeIds, now unused
            }
        }

        public NodesStatsRequestParameters getNodesStatsRequestParameters() {
            return nodesStatsRequestParameters;
        }
    }
}
