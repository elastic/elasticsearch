/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.WriteRequest;

import java.util.List;
import java.util.Objects;

public class BulkDeleteRolesRequest extends LegacyActionRequest {

    private List<String> roleNames;

    public BulkDeleteRolesRequest(List<String> roleNames) {
        this.roleNames = roleNames;
    }

    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

    @Override
    public ActionRequestValidationException validate() {
        // Handle validation where delete role is handled to produce partial success if validation fails
        return null;
    }

    public List<String> getRoleNames() {
        return roleNames;
    }

    public BulkDeleteRolesRequest setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass() || super.equals(o)) return false;

        BulkDeleteRolesRequest that = (BulkDeleteRolesRequest) o;
        return Objects.equals(roleNames, that.roleNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleNames);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }
}
