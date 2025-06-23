/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class IndexModeStatsActionType extends ActionType<IndexModeStatsActionType.StatsResponse> {
    public static final IndexModeStatsActionType TYPE = new IndexModeStatsActionType();

    private IndexModeStatsActionType() {
        super("cluster:monitor/nodes/index_mode_stats");
    }

    public static final class StatsRequest extends BaseNodesRequest {
        public StatsRequest(String[] nodesIds) {
            super(nodesIds);
        }

        public StatsRequest(DiscoveryNode... concreteNodes) {
            super(concreteNodes);
        }
    }

    public static final class StatsResponse extends BaseNodesResponse<NodeResponse> {
        StatsResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "must be local";
            throw new UnsupportedOperationException("must be local");
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            assert false : "must be local";
            throw new UnsupportedOperationException("must be local");
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            assert false : "must be local";
            throw new UnsupportedOperationException("must be local");
        }

        public Map<IndexMode, IndexStats> stats() {
            final Map<IndexMode, IndexStats> stats = new EnumMap<>(IndexMode.class);
            for (IndexMode mode : IndexMode.values()) {
                stats.put(mode, new IndexStats());
            }
            for (NodeResponse node : getNodes()) {
                for (Map.Entry<IndexMode, IndexStats> e : node.stats.entrySet()) {
                    stats.get(e.getKey()).add(e.getValue());
                }
            }
            return stats;
        }
    }

    public static final class NodeRequest extends AbstractTransportRequest {
        NodeRequest() {

        }

        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final Map<IndexMode, IndexStats> stats;

        NodeResponse(DiscoveryNode node, Map<IndexMode, IndexStats> stats) {
            super(node);
            this.stats = stats;
        }

        NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
            stats = in.readMap(IndexMode::readFrom, IndexStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(stats, (o, m) -> IndexMode.writeTo(m, o), (o, s) -> s.writeTo(o));
        }
    }

    public static class TransportAction extends TransportNodesAction<StatsRequest, StatsResponse, NodeRequest, NodeResponse, Void> {
        private final IndicesService indicesService;

        @Inject
        public TransportAction(
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndicesService indicesService
        ) {
            super(
                TYPE.name(),
                clusterService,
                transportService,
                actionFilters,
                NodeRequest::new,
                transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
            );
            this.indicesService = indicesService;
        }

        @Override
        protected StatsResponse newResponse(StatsRequest request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
            return new StatsResponse(ClusterName.DEFAULT, nodeResponses, failures);
        }

        @Override
        protected NodeRequest newNodeRequest(StatsRequest request) {
            return new NodeRequest();
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new NodeResponse(in, node);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            return new NodeResponse(clusterService.localNode(), IndicesMetrics.getStatsWithoutCache(indicesService));
        }
    }
}
