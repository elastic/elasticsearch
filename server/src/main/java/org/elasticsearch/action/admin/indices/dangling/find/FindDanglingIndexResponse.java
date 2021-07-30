/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Models a response to a {@link FindDanglingIndexRequest}. A find request queries every node in the
 * cluster looking for a dangling index with a specific UUID.
 */
public class FindDanglingIndexResponse extends BaseNodesResponse<NodeFindDanglingIndexResponse> {

    public FindDanglingIndexResponse(StreamInput in) throws IOException {
        super(in);
    }

    public FindDanglingIndexResponse(
        ClusterName clusterName,
        List<NodeFindDanglingIndexResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeFindDanglingIndexResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeFindDanglingIndexResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeFindDanglingIndexResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
