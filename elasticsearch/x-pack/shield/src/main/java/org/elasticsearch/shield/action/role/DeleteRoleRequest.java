/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request delete a role from the shield index
 */
public class DeleteRoleRequest extends ActionRequest<DeleteRoleRequest> {

    private String role;

    public DeleteRoleRequest() {
    }

    public DeleteRoleRequest(String roleName) {
        this.role = roleName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (role == null) {
            validationException = addValidationError("role is missing", validationException);
        }
        return validationException;
    }

    public void role(String role) {
        this.role = role;
    }

    public String role() {
        return role;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        role = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(role);
    }
}
