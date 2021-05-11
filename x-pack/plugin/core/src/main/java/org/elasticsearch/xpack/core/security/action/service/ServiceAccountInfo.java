/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Objects;

public class ServiceAccountInfo implements Writeable, ToXContent {

    private final String principal;
    private final RoleDescriptor roleDescriptor;

    public ServiceAccountInfo(String principal, RoleDescriptor roleDescriptor) {
        this.principal = Objects.requireNonNull(principal, "service account principal cannot be null");
        this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "service account descriptor cannot be null");
    }

    public ServiceAccountInfo(StreamInput in) throws IOException {
        this.principal = in.readString();
        this.roleDescriptor = new RoleDescriptor(in);
    }

    public String getPrincipal() {
        return principal;
    }

    public RoleDescriptor getRoleDescriptor() {
        return roleDescriptor;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        roleDescriptor.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(principal);
        builder.field("role_descriptor");
        roleDescriptor.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ServiceAccountInfo{" + "principal='" + principal + '\'' + ", roleDescriptor=" + roleDescriptor + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ServiceAccountInfo that = (ServiceAccountInfo) o;
        return principal.equals(that.principal) && roleDescriptor.equals(that.roleDescriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, roleDescriptor);
    }
}
