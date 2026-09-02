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

/**
 * What the service account API reports about one account. The two kinds of account describe their privileges
 * differently. A built-in account carries the fixed {@link RoleDescriptor} it was declared with, a user-managed one
 * carries the names of the roles it was created with plus whether it is enabled. So this is a union tagged by
 * {@link #managedBy()}.
 */
public sealed interface ServiceAccountInfo extends Writeable, ToXContent {

    /**
     * Gates the tag and everything that follows it. Before this version the wire form was a principal followed
     * directly by a role descriptor, which is what {@link BuiltIn} still writes to such a node.
     */
    TransportVersion USER_MANAGED_SERVICE_ACCOUNT_INFO = TransportVersion.fromName("user_managed_service_account_info");

    String principal();

    ServiceAccountManagedBy managedBy();

    /**
     * A built-in account, whose privileges are the role descriptor declared for it in the Elasticsearch distribution.
     */
    record BuiltIn(String principal, RoleDescriptor roleDescriptor) implements ServiceAccountInfo {

        public BuiltIn {
            Objects.requireNonNull(principal, "service account principal cannot be null");
            Objects.requireNonNull(roleDescriptor, "service account role descriptor cannot be null");
        }

        @Override
        public ServiceAccountManagedBy managedBy() {
            return ServiceAccountManagedBy.ELASTIC;
        }
    }

    /**
     * An account created through the API, whose privileges are the named roles resolved when it authenticates. The
     * roles are reported as the caller gave them.
     */
    record UserManaged(String principal, List<String> roles, boolean enabled) implements ServiceAccountInfo {

        public UserManaged {
            Objects.requireNonNull(principal, "service account principal cannot be null");
            roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        }

        @Override
        public ServiceAccountManagedBy managedBy() {
            return ServiceAccountManagedBy.USER;
        }
    }

    static ServiceAccountInfo readFrom(StreamInput in) throws IOException {
        final String principal = in.readString();
        if (in.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_INFO) == false) {
            return new BuiltIn(principal, new RoleDescriptor(in));
        }
        return switch (in.readEnum(ServiceAccountManagedBy.class)) {
            case ELASTIC -> new BuiltIn(principal, new RoleDescriptor(in));
            case USER -> new UserManaged(principal, in.readStringCollectionAsList(), in.readBoolean());
        };
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        final boolean tagged = out.getTransportVersion().supports(USER_MANAGED_SERVICE_ACCOUNT_INFO);
        // Unreachable: a node that cannot read the tag also cannot ask for user-managed accounts, since its request
        // arrives with a managed_by of just [elastic]. Stated as a failure rather than an assertion so that a future
        // caller that gets this wrong is told, instead of writing a truncated account into the stream.
        if (tagged == false && this instanceof BuiltIn == false) {
            throw new IllegalStateException(
                "cannot send information about the user-managed service account ["
                    + principal()
                    + "] to a node that does not support user-managed service accounts"
            );
        }
        out.writeString(principal());
        if (tagged) {
            out.writeEnum(managedBy());
        }
        switch (this) {
            case BuiltIn builtIn -> builtIn.roleDescriptor().writeTo(out);
            case UserManaged userManaged -> {
                out.writeStringCollection(userManaged.roles());
                out.writeBoolean(userManaged.enabled());
            }
        }
    }

    /**
     * Renders the account as a field named for its principal, so that a response can hold many of them.
     */
    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(principal());
        builder.field("managed_by", managedBy().value());
        switch (this) {
            case BuiltIn builtIn -> {
                builder.field("role_descriptor");
                builtIn.roleDescriptor().toXContent(builder, params);
            }
            case UserManaged userManaged -> {
                builder.stringListField("roles", userManaged.roles());
                builder.field("enabled", userManaged.enabled());
            }
        }
        return builder.endObject();
    }
}
