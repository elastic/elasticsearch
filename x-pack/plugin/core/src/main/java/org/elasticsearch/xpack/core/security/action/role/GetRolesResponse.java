/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

/**
 * A response for the {@code Get Roles} API that holds the retrieved role descriptors.
 */
public class GetRolesResponse extends ActionResponse {

    private RoleDescriptor[] roles;

    public GetRolesResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        roles = new RoleDescriptor[size];
        for (int i = 0; i < size; i++) {
            roles[i] = new RoleDescriptor(in);
        }
    }

    public GetRolesResponse(RoleDescriptor... roles) {
        this.roles = roles;
    }

    public RoleDescriptor[] roles() {
        return roles;
    }

    public boolean hasRoles() {
        return roles.length > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(roles.length);
        for (RoleDescriptor role : roles) {
            role.writeTo(out);
        }
    }
}
