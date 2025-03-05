/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.enrich.EnrichCache;

import java.io.IOException;
import java.util.List;

/**
 * This is an internal action that gather coordinator stats from each node with an ingest role in the cluster.
 * This action is only used via the {@link EnrichStatsAction}.
 */
public class EnrichCoordinatorStatsAction extends ActionType<EnrichCoordinatorStatsAction.Response> {

    public static final EnrichCoordinatorStatsAction INSTANCE = new EnrichCoordinatorStatsAction();
    public static final String NAME = "cluster:monitor/xpack/enrich/coordinator_stats";

    private EnrichCoordinatorStatsAction() {
        super(NAME);
    }

    // This always executes on all ingest nodes, hence no node ids need to be provided.
    public static class Request extends BaseNodesRequest {
        public Request() {
            super(new String[0]);
        }
    }

    public static class NodeRequest extends TransportRequest {

        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

    }

    public static class Response extends BaseNodesResponse<NodeResponse> {

        Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return org.elasticsearch.action.support.TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            org.elasticsearch.action.support.TransportAction.localOnly();
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final EnrichStatsAction.Response.CacheStats cacheStats;
        private final CoordinatorStats coordinatorStats;

        NodeResponse(DiscoveryNode node, EnrichStatsAction.Response.CacheStats cacheStats, CoordinatorStats coordinatorStats) {
            super(node);
            this.cacheStats = cacheStats;
            this.coordinatorStats = coordinatorStats;
        }

        NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.cacheStats = new EnrichStatsAction.Response.CacheStats(in);
            this.coordinatorStats = new CoordinatorStats(in);
        }

        public CoordinatorStats getCoordinatorStats() {
            return coordinatorStats;
        }

        public EnrichStatsAction.Response.CacheStats getCacheStats() {
            return cacheStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            cacheStats.writeTo(out);
            coordinatorStats.writeTo(out);
        }
    }

    public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse, Void> {

        private final EnrichCache enrichCache;
        private final EnrichCoordinatorProxyAction.Coordinator coordinator;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            EnrichCache enrichCache,
            EnrichCoordinatorProxyAction.Coordinator coordinator
        ) {
            super(NAME, clusterService, transportService, actionFilters, NodeRequest::new, threadPool.executor(ThreadPool.Names.GENERIC));
            this.enrichCache = enrichCache;
            this.coordinator = coordinator;
        }

        @Override
        protected DiscoveryNode[] resolveRequest(Request request, ClusterState clusterState) {
            return clusterState.getNodes().getIngestNodes().values().toArray(DiscoveryNode[]::new);
        }

        @Override
        protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
            return new Response(clusterService.getClusterName(), nodeResponses, failures);
        }

        @Override
        protected NodeRequest newNodeRequest(Request request) {
            return new NodeRequest();
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            DiscoveryNode node = clusterService.localNode();
            return new NodeResponse(node, enrichCache.getStats(node.getId()), coordinator.getStats(node.getId()));
        }
    }

}
