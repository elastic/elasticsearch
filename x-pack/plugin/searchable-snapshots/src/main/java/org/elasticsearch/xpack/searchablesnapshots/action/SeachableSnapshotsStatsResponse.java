/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats;

import java.io.IOException;
import java.util.List;

public class SeachableSnapshotsStatsResponse
    extends BaseNodesResponse<SeachableSnapshotsStatsResponse.NodeStatsResponse> implements ToXContentObject {

    SeachableSnapshotsStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    SeachableSnapshotsStatsResponse(ClusterName clusterName, List<NodeStatsResponse> nodes, List<FailedNodeException> failures) {
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
        builder.startObject("nodes");
        for (NodeStatsResponse node : getNodes()) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    static class NodeStatsResponse extends BaseNodeResponse implements ToXContentFragment {

        private final SearchableSnapshotStats stats;

        NodeStatsResponse(StreamInput in) throws IOException {
            super(in);
            this.stats = in.readOptionalWriteable(SearchableSnapshotStats::new);
        }

        NodeStatsResponse(DiscoveryNode node, SearchableSnapshotStats stats) {
            super(node);
            this.stats = stats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(stats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (stats != null && stats.isEmpty() == false) {
                builder.field(getNode().getId(), stats);
            }
            return builder;
        }

        static NodeStatsResponse readNodeResponse(StreamInput in) throws IOException {
            return new NodeStatsResponse(in);
        }
    }
}
