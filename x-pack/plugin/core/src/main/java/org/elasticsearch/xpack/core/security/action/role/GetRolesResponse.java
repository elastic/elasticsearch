/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

/**
 * A response for the {@code Get Roles} API that holds the retrieved role descriptors.
 */
public class GetRolesResponse extends ActionResponse {

    private final RoleDescriptor[] roles;

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
        TransportAction.localOnly();
    }
}
