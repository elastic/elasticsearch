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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        if (outerParams.param(DenseVectorStats.INCLUDE_OFF_HEAP) == null) {
            outerParams = new ToXContent.DelegatingMapParams(Map.of(DenseVectorStats.INCLUDE_OFF_HEAP, "true"), outerParams);
        }
        var finalOuterParams = new ToXContent.DelegatingMapParams(Map.of(DenseVectorStats.INCLUDE_OFF_HEAP, "true"), outerParams);
        return ChunkedToXContentHelper.object(
            "nodes",
            Iterators.flatMap(getNodes().iterator(), nodeStats -> Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject(nodeStats.getNode().getId());
                builder.field("timestamp", nodeStats.getTimestamp());
                return builder;
            }), nodeStats.toXContentChunked(finalOuterParams), ChunkedToXContentHelper.endObject()))
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
