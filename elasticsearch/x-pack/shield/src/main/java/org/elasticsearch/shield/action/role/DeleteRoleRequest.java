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

    private String name;
    private boolean refresh = true;

    public DeleteRoleRequest() {
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

    public void refresh(boolean refresh) {
        this.refresh = refresh;
    }

    public boolean refresh() {
        return refresh;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeBoolean(refresh);
    }
}
