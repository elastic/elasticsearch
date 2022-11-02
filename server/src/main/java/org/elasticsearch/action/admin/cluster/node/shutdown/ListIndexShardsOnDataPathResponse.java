/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ListIndexShardsOnDataPathResponse extends BaseNodesResponse<NodeListIndexShardsOnDataPathResponse> {

    public ListIndexShardsOnDataPathResponse(
        ClusterName clusterName,
        List<NodeListIndexShardsOnDataPathResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public ListIndexShardsOnDataPathResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<NodeListIndexShardsOnDataPathResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeListIndexShardsOnDataPathResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeListIndexShardsOnDataPathResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
