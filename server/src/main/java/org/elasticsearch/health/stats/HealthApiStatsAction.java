/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.stats;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This class collects the stats of the health API from every node
 */
public class HealthApiStatsAction extends ActionType<HealthApiStatsAction.Response> {

    public static final HealthApiStatsAction INSTANCE = new HealthApiStatsAction();
    public static final String NAME = "cluster:monitor/health_api/stats";

    private HealthApiStatsAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest {

        public Request() {
            super((String[]) null);
        }

        @Override
        public String toString() {
            return "health_api_stats";
        }

        public static class Node extends TransportRequest {

            public Node(StreamInput in) throws IOException {
                super(in);
            }

            public Node(Request ignored) {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
            }
        }
    }

    public static class Response extends BaseNodesResponse<Response.Node> {

        public Response(ClusterName clusterName, List<Node> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        protected List<Node> readNodesFrom(StreamInput in) throws IOException {
            return TransportAction.localOnly();
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
            TransportAction.localOnly();
        }

        public Counters getStats() {
            List<Counters> counters = getNodes().stream().map(Node::getStats).filter(Objects::nonNull).toList();
            return Counters.merge(counters);
        }

        public static class Node extends BaseNodeResponse {
            @Nullable
            private Counters stats;

            public Node(StreamInput in) throws IOException {
                super(in);
                stats = in.readOptionalWriteable(Counters::new);
            }

            public Node(DiscoveryNode node) {
                super(node);
            }

            public Counters getStats() {
                return stats;
            }

            public void setStats(Counters stats) {
                this.stats = stats;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalWriteable(stats);
            }
        }
    }
}
