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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ServiceAccountInfo implements Writeable, ToXContent {

    public static final TransportVersion MANAGED_SERVICE_ACCOUNTS = TransportVersion.fromName("managed_service_accounts");

    private final String principal;
    private final boolean managed;
    private final RoleDescriptor roleDescriptor;
    private final List<String> roles;
    private final Boolean enabled;

    public static ServiceAccountInfo builtIn(String principal, RoleDescriptor roleDescriptor) {
        return new ServiceAccountInfo(principal, false, roleDescriptor, null, null);
    }

    public static ServiceAccountInfo managed(String principal, List<String> roles, boolean enabled) {
        return new ServiceAccountInfo(principal, true, null, List.copyOf(roles), enabled);
    }

    private ServiceAccountInfo(String principal, boolean managed, RoleDescriptor roleDescriptor, List<String> roles, Boolean enabled) {
        this.principal = Objects.requireNonNull(principal, "service account principal cannot be null");
        this.managed = managed;
        if (managed) {
            this.roleDescriptor = null;
            this.roles = Objects.requireNonNull(roles, "roles cannot be null");
            this.enabled = Objects.requireNonNull(enabled, "enabled cannot be null");
        } else {
            this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "service account descriptor cannot be null");
            this.roles = null;
            this.enabled = null;
        }
    }

    public ServiceAccountInfo(StreamInput in) throws IOException {
        this.principal = in.readString();
        if (in.getTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            this.managed = in.readBoolean();
            if (managed) {
                this.roles = in.readStringCollectionAsList();
                this.enabled = in.readBoolean();
                this.roleDescriptor = null;
            } else {
                this.roleDescriptor = new RoleDescriptor(in);
                this.roles = null;
                this.enabled = null;
            }
        } else {
            this.managed = false;
            this.roleDescriptor = new RoleDescriptor(in);
            this.roles = null;
            this.enabled = null;
        }
    }

    public String getPrincipal() {
        return principal;
    }

    public boolean isManaged() {
        return managed;
    }

    public RoleDescriptor getRoleDescriptor() {
        return roleDescriptor;
    }

    public List<String> getRoles() {
        return roles;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        if (out.getTransportVersion().supports(MANAGED_SERVICE_ACCOUNTS)) {
            out.writeBoolean(managed);
            if (managed) {
                out.writeStringCollection(roles);
                out.writeBoolean(enabled);
            } else {
                roleDescriptor.writeTo(out);
            }
        } else {
            assert managed == false : "cannot serialize managed service account info to older transport version";
            roleDescriptor.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(principal);
        if (managed) {
            builder.field("managed_by", ServiceAccountManagedBy.USER.value());
            builder.stringListField("roles", roles);
            builder.field("enabled", enabled);
        } else {
            builder.field("managed_by", ServiceAccountManagedBy.ELASTIC.value());
            builder.field("role_descriptor");
            roleDescriptor.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ServiceAccountInfo{"
            + "principal='"
            + principal
            + '\''
            + ", managed="
            + managed
            + ", roleDescriptor="
            + roleDescriptor
            + ", roles="
            + roles
            + ", enabled="
            + enabled
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceAccountInfo that = (ServiceAccountInfo) o;
        return managed == that.managed
            && principal.equals(that.principal)
            && Objects.equals(roleDescriptor, that.roleDescriptor)
            && Objects.equals(roles, that.roles)
            && Objects.equals(enabled, that.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, managed, roleDescriptor, roles, enabled);
    }
}
