/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesXContentResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class NodesStatsResponse extends BaseNodesXContentResponse<NodeStats> {

    public NodesStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesStatsResponse(ClusterName clusterName, List<NodeStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    protected Iterator<? extends ToXContent> xContentChunks() {
        return Iterators.concat(
            Iterators.single((b, p) -> b.startObject("nodes")),
            getNodes().stream().map(nodeStats -> (ToXContent) (b, p) -> {
                b.startObject(nodeStats.getNode().getId());
                b.field("timestamp", nodeStats.getTimestamp());
                nodeStats.toXContent(b, p);
                return b.endObject();
            }).iterator(),
            Iterators.single((b, p) -> b.endObject())
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
