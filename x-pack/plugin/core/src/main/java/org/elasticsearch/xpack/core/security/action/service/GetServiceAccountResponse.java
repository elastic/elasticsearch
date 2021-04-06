/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetServiceAccountResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, RoleDescriptor> serviceAccounts;

    public GetServiceAccountResponse(Map<String, RoleDescriptor> serviceAccounts) {
        this.serviceAccounts = Objects.requireNonNull(serviceAccounts);
    }

    public GetServiceAccountResponse(StreamInput in) throws IOException {
        super(in);
        this.serviceAccounts = in.readMap(StreamInput::readString, RoleDescriptor::new);
    }

    public Map<String, RoleDescriptor> getServiceAccounts() {
        return serviceAccounts;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(serviceAccounts, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, RoleDescriptor> entry: serviceAccounts.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("role_descriptor");
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountResponse that = (GetServiceAccountResponse) o;
        return serviceAccounts.equals(that.serviceAccounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAccounts);
    }
}
