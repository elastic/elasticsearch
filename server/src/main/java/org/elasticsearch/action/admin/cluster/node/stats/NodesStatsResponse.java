/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class NodesStatsResponse extends BaseNodesResponse<NodeStats> implements ChunkedToXContentObject {

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
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(

            ChunkedToXContentHelper.singleChunk((b, p) -> {
                b.startObject();
                RestActions.buildNodesHeader(b, p, this);
                return b.field("cluster_name", getClusterName().value()).startObject("nodes");
            }),
            Iterators.flatMap(
                getNodes().iterator(),
                nodeStats -> Iterators.concat(
                    ChunkedToXContentHelper.singleChunk(
                        (b, p) -> b.startObject(nodeStats.getNode().getId()).field("timestamp", nodeStats.getTimestamp())
                    ),
                    nodeStats.toXContentChunked(params),
                    ChunkedToXContentHelper.endObject()
                )
            ),
            ChunkedToXContentHelper.singleChunk((b, p) -> b.endObject().endObject())
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
