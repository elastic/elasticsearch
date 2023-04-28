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

public class PrevalidateShardPathResponse extends BaseNodesResponse<NodePrevalidateShardPathResponse> {

    public PrevalidateShardPathResponse(
        ClusterName clusterName,
        List<NodePrevalidateShardPathResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public PrevalidateShardPathResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<NodePrevalidateShardPathResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodePrevalidateShardPathResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodePrevalidateShardPathResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
