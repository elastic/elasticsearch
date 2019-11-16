/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetFileRolesResponse extends BaseNodesResponse<GetFileRolesAction.NodeResponse> {

    public GetFileRolesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetFileRolesResponse(ClusterName clusterName, List<GetFileRolesAction.NodeResponse> nodes,
                                   List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<GetFileRolesAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetFileRolesAction.NodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<GetFileRolesAction.NodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetFileRolesResponse that = (GetFileRolesResponse) o;
        return Objects.equals(getClusterName(), that.getClusterName())
            && Objects.equals(getNodes(), that.getNodes())
            && Objects.equals(failures(), that.failures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterName(), getNodes(), failures());
    }

    @Override
    public String toString() {
        return "GetFileRolesResponse[nodeResponses= " + this.getNodes().toString() + "]";
    }
}
