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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;

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
        super(NAME, Response::new);
    }

    // This always executes on all ingest nodes, hence no node ids need to be provided.
    public static class Request extends BaseNodesRequest<Request> {

        public Request() {
            super(new String[0]);
        }

        Request(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class NodeRequest extends TransportRequest {

        NodeRequest() {}

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

    }

    public static class Response extends BaseNodesResponse<NodeResponse> {

        Response(StreamInput in) throws IOException {
            super(in);
        }

        Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final CoordinatorStats coordinatorStats;

        NodeResponse(DiscoveryNode node, CoordinatorStats coordinatorStats) {
            super(node);
            this.coordinatorStats = coordinatorStats;
        }

        NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.coordinatorStats = new CoordinatorStats(in);
        }

        public CoordinatorStats getCoordinatorStats() {
            return coordinatorStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            coordinatorStats.writeTo(out);
        }
    }

    public static class TransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse> {

        private final EnrichCoordinatorProxyAction.Coordinator coordinator;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            EnrichCoordinatorProxyAction.Coordinator coordinator
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                Request::new,
                NodeRequest::new,
                ThreadPool.Names.SAME,
                NodeResponse.class
            );
            this.coordinator = coordinator;
        }

        @Override
        protected void resolveRequest(Request request, ClusterState clusterState) {
            DiscoveryNode[] ingestNodes = clusterState.getNodes().getIngestNodes().values().toArray(DiscoveryNode.class);
            request.setConcreteNodes(ingestNodes);
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
        protected NodeResponse newNodeResponse(StreamInput in) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            DiscoveryNode node = clusterService.localNode();
            return new NodeResponse(node, coordinator.getStats(node.getId()));
        }
    }

}
