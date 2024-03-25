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

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request delete a role from the security index
 */
public class DeleteRoleRequest extends ActionRequest implements WriteRequest<DeleteRoleRequest> {

    private String name;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    private boolean restrictRequest;

    public DeleteRoleRequest() {}

    @Override
    public DeleteRoleRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("role name is missing", validationException);
        }
        return validationException;
    }

    public void name(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public void restrictRequest(boolean restrictRequest) {
        this.restrictRequest = restrictRequest;
    }

    public boolean restrictRequest() {
        return restrictRequest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
