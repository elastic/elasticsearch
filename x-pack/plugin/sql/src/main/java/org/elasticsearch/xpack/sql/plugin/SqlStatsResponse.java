/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.io.IOException;
import java.util.List;

public class SqlStatsResponse extends BaseNodesResponse<SqlStatsResponse.NodeStatsResponse> implements ToXContentObject {

    public SqlStatsResponse(ClusterName clusterName, List<NodeStatsResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStatsResponse> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStatsResponse> nodes) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("stats");
        for (NodeStatsResponse node : getNodes()) {
            node.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }

    public static class NodeStatsResponse extends BaseNodeResponse implements ToXContentObject {

        private Counters stats;

        public NodeStatsResponse(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                stats = new Counters(in);
            }
        }

        public NodeStatsResponse(DiscoveryNode node) {
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
            out.writeBoolean(stats != null);
            if (stats != null) {
                stats.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (stats != null && stats.hasCounters()) {
                builder.field("stats", stats.toNestedMap());
            }
            builder.endObject();
            return builder;
        }

        static SqlStatsResponse.NodeStatsResponse readNodeResponse(StreamInput in) throws IOException {
            return new SqlStatsResponse.NodeStatsResponse(in);
        }

    }
}
