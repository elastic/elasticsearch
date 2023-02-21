/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * The response for the nodes usage api which contains the individual usage
 * statistics for all nodes queried.
 */
public class NodesUsageResponse extends BaseNodesResponse<NodeUsage> implements ToXContentFragment {

    public NodesUsageResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesUsageResponse(ClusterName clusterName, List<NodeUsage> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeUsage> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeUsage::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeUsage> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (NodeUsage nodeUsage : getNodes()) {
            builder.startObject(nodeUsage.getNode().getId());
            builder.field("timestamp", nodeUsage.getTimestamp());
            nodeUsage.toXContent(builder, params);

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
