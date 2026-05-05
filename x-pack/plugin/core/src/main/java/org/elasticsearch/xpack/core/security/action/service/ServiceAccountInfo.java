/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Objects;

public class ServiceAccountInfo implements Writeable, ToXContent {

    private static final TransportVersion USER_DEFINED_SERVICE_ACCOUNTS = TransportVersion.fromName("user_defined_service_accounts");

    public enum Type {
        BUILT_IN("built-in"),
        USER_DEFINED("user-defined");

        private final String wireName;

        Type(String wireName) {
            this.wireName = wireName;
        }

        public String wireName() {
            return wireName;
        }
    }

    private final String principal;
    private final RoleDescriptor roleDescriptor;
    @Nullable
    private final Type type;

    public ServiceAccountInfo(String principal, RoleDescriptor roleDescriptor) {
        this(principal, roleDescriptor, null);
    }

    public ServiceAccountInfo(String principal, RoleDescriptor roleDescriptor, @Nullable Type type) {
        this.principal = Objects.requireNonNull(principal, "service account principal cannot be null");
        this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "service account descriptor cannot be null");
        this.type = type;
    }

    public ServiceAccountInfo(StreamInput in) throws IOException {
        this.principal = in.readString();
        this.roleDescriptor = new RoleDescriptor(in);
        if (in.getTransportVersion().supports(USER_DEFINED_SERVICE_ACCOUNTS)) {
            this.type = in.readOptionalEnum(Type.class);
        } else {
            this.type = null;
        }
    }

    public String getPrincipal() {
        return principal;
    }

    public RoleDescriptor getRoleDescriptor() {
        return roleDescriptor;
    }

    @Nullable
    public Type getType() {
        return type;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        roleDescriptor.writeTo(out);
        if (out.getTransportVersion().supports(USER_DEFINED_SERVICE_ACCOUNTS)) {
            out.writeOptionalEnum(type);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(principal);
        if (type != null) {
            builder.field("type", type.wireName());
        }
        builder.field("role_descriptor");
        roleDescriptor.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ServiceAccountInfo{principal='" + principal + "', type=" + type + ", roleDescriptor=" + roleDescriptor + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceAccountInfo that = (ServiceAccountInfo) o;
        return principal.equals(that.principal) && roleDescriptor.equals(that.roleDescriptor) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, roleDescriptor, type);
    }
}
