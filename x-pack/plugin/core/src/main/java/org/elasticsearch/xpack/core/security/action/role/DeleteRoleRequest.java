/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils.AuditToXContentParams;

/**
 * A request delete a role from the security index
 */
public class DeleteRoleRequest extends ActionRequest implements WriteRequest<DeleteRoleRequest>, ToXContentObject {

    private String name;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public DeleteRoleRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public DeleteRoleRequest() {
    }

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        refreshPolicy.writeTo(out);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
               .startObject("delete_role")
               .field("name", name())
               .endObject();
        if (params.paramAsBoolean(AuditToXContentParams.INCLUDE_REFRESH_POLICY, false)) {
            builder.field("refresh_policy", refreshPolicy.toString());
        }
        return builder.endObject();
    }
}
