/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class GpuStatsResponse extends BaseNodesResponse<NodeGpuStatsResponse> {

    public GpuStatsResponse(ClusterName clusterName, List<NodeGpuStatsResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeGpuStatsResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readCollectionAsList(NodeGpuStatsResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeGpuStatsResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }
}
