/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NodesDeprecationCheckResponse extends BaseNodesResponse<NodesDeprecationCheckAction.NodeResponse> {

    public NodesDeprecationCheckResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesDeprecationCheckResponse(ClusterName clusterName,
                                         List<NodesDeprecationCheckAction.NodeResponse> nodes,
                                         List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodesDeprecationCheckAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodesDeprecationCheckAction.NodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodesDeprecationCheckAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodesDeprecationCheckResponse that = (NodesDeprecationCheckResponse) o;
        return Objects.equals(getClusterName(), that.getClusterName())
            && Objects.equals(getNodes(), that.getNodes())
            && Objects.equals(failures(), that.failures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterName(), getNodes(), failures());
    }
}
