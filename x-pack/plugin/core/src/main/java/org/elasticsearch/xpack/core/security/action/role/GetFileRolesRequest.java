/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class GetFileRolesRequest extends BaseNodesRequest<GetFileRolesRequest> {
    public GetFileRolesRequest(StreamInput in) throws IOException {
        super(in);
    }

    public GetFileRolesRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object[]) this.nodesIds());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        GetFileRolesRequest that = (GetFileRolesRequest) obj;
        return Arrays.equals(this.nodesIds(), that.nodesIds());
    }

    @Override
    public String toString() {
        return "GetFileRolesRequest[nodes: " + Arrays.toString(this.nodesIds()) + "]";
    }
}
