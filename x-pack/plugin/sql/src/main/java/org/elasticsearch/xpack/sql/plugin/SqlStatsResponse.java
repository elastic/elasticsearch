/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.stats.Counters;

import java.io.IOException;
import java.util.List;

public class SqlStatsResponse extends BaseNodesResponse<SqlStatsResponse.Node> implements ToXContentObject {
    
    public SqlStatsResponse() {
    }
    
    public SqlStatsResponse(ClusterName clusterName, List<Node> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<Node> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(Node::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("stats");
        for (Node node : getNodes()) {
            node.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }

    public static class Node extends BaseNodeResponse implements ToXContentObject {
        
        private Counters stats;
        
        public Node() {
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
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                stats = Counters.read(in);
            }
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
        
        static SqlStatsResponse.Node readNodeResponse(StreamInput in) throws IOException {
            SqlStatsResponse.Node node = new SqlStatsResponse.Node();
            node.readFrom(in);
            return node;
        }

    }
}
