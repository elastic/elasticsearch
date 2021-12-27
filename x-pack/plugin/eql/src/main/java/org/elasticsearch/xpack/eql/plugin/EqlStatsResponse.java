/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.FailedNodeException;
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

public class EqlStatsResponse extends BaseNodesResponse<EqlStatsResponse.NodeStatsResponse> implements ToXContentObject {

    public EqlStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public EqlStatsResponse(ClusterName clusterName, List<NodeStatsResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStatsResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeStatsResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStatsResponse> nodes) throws IOException {
        out.writeList(nodes);
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

        static EqlStatsResponse.NodeStatsResponse readNodeResponse(StreamInput in) throws IOException {
            return new EqlStatsResponse.NodeStatsResponse(in);
        }

    }
}
