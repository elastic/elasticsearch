/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class BulkPutRolesRequest extends ActionRequest {

    private List<RoleDescriptor> roles;

    public BulkPutRolesRequest(List<RoleDescriptor> roles) {
        this.roles = roles;
    }

    public void setRoles(List<RoleDescriptor> roles) {
        this.roles = roles;
    }

    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

    @Override
    public ActionRequestValidationException validate() {
        // Handle validation where put role is handled to produce partial success if validation fails
        return null;
    }

    public List<RoleDescriptor> getRoles() {
        return roles;
    }

    public BulkPutRolesRequest setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass() || super.equals(o)) return false;

        BulkPutRolesRequest that = (BulkPutRolesRequest) o;
        return Objects.equals(roles, that.roles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roles);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
