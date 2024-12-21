/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesXContentResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NewChunkedXContentBuilder;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.collect.Iterators.flatMap;
import static org.elasticsearch.common.xcontent.NewChunkedXContentBuilder.chunk;

public class NodesStatsResponse extends BaseNodesXContentResponse<NodeStats> {

    public NodesStatsResponse(ClusterName clusterName, List<NodeStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStats> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStats> nodes) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    protected Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams) {
        return NewChunkedXContentBuilder.object(
            "nodes",
            flatMap(
                getNodes().iterator(),
                ns -> NewChunkedXContentBuilder.object(
                    ns.getNode().getId(),
                    chunk((b, p) -> b.field("timestamp", ns.getTimestamp())),
                    ns.toXContentChunked(outerParams)
                )
            )
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
