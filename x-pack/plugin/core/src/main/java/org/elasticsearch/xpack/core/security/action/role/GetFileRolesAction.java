/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetFileRolesAction extends ActionType<GetFileRolesResponse> {
    public static final GetFileRolesAction INSTANCE = new GetFileRolesAction();
    public static final String NAME = "cluster:admin/xpack/security/role/file/get";


    private GetFileRolesAction() {
        super(NAME, GetFileRolesResponse::new);
    }

    public static class NodeRequest extends BaseNodeRequest {
        GetFileRolesRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new GetFileRolesRequest(in);
        }

        public NodeRequest(GetFileRolesRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeRequest that = (NodeRequest) o;
            return Objects.equals(request, that.request);
        }

        @Override
        public int hashCode() {
            return Objects.hash(request);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final List<RoleDescriptor> roles;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            roles = in.readList(RoleDescriptor::new);
        }

        public NodeResponse(DiscoveryNode node, List<RoleDescriptor> roles) {
            super(node);
            this.roles = roles;
        }

        public List<RoleDescriptor> getRoles() {
            return roles;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(roles);
        }

        @Override
        public String toString() {
            return "GetFileRolesNodeResponse[roles=" + roles.toString() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return Objects.equals(this.getNode(), that.getNode())
                && Objects.equals(roles, that.roles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), roles);
        }
    }
}
