/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request delete a role-mapping from the org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class DeleteRoleMappingRequest extends LegacyActionRequest implements WriteRequest<DeleteRoleMappingRequest> {

    private String name;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public DeleteRoleMappingRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public DeleteRoleMappingRequest() {}

    @Override
    public DeleteRoleMappingRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name == null) {
            return addValidationError("role-mapping name is missing", null);
        } else {
            return null;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        refreshPolicy.writeTo(out);
    }
}
