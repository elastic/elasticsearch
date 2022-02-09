/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Request to create API key
 */
public final class CreateApiKeyRequest implements Validatable, ToXContentObject {

    private final String name;
    private final TimeValue expiration;
    private final List<Role> roles;
    private final RefreshPolicy refreshPolicy;
    private final Map<String, Object> metadata;

    /**
     * Create API Key request constructor
     * @param name name for the API key
     * @param roles list of {@link Role}s
     * @param expiration to specify expiration for the API key
     * @param metadata Arbitrary metadata for the API key
     */
    public CreateApiKeyRequest(
        String name,
        List<Role> roles,
        @Nullable TimeValue expiration,
        @Nullable final RefreshPolicy refreshPolicy,
        @Nullable Map<String, Object> metadata
    ) {
        this.name = name;
        this.roles = Objects.requireNonNull(roles, "roles may not be null");
        this.expiration = expiration;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
        this.metadata = metadata;
    }

    public CreateApiKeyRequest(String name, List<Role> roles, @Nullable TimeValue expiration, @Nullable final RefreshPolicy refreshPolicy) {
        this(name, roles, expiration, refreshPolicy, null);
    }

    public String getName() {
        return name;
    }

    public TimeValue getExpiration() {
        return expiration;
    }

    public List<Role> getRoles() {
        return roles;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, refreshPolicy, roles, expiration, metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CreateApiKeyRequest that = (CreateApiKeyRequest) o;
        return Objects.equals(name, that.name)
            && Objects.equals(refreshPolicy, that.refreshPolicy)
            && Objects.equals(roles, that.roles)
            && Objects.equals(expiration, that.expiration)
            && Objects.equals(metadata, that.metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("name", name);
        if (expiration != null) {
            builder.field("expiration", expiration.getStringRep());
        }
        builder.startObject("role_descriptors");
        for (Role role : roles) {
            builder.startObject(role.getName());
            if (role.getApplicationPrivileges() != null) {
                builder.field(Role.APPLICATIONS.getPreferredName(), role.getApplicationPrivileges());
            }
            if (role.getClusterPrivileges() != null) {
                builder.field(Role.CLUSTER.getPreferredName(), role.getClusterPrivileges());
            }
            if (role.getGlobalPrivileges() != null) {
                builder.field(Role.GLOBAL.getPreferredName(), role.getGlobalPrivileges());
            }
            if (role.getIndicesPrivileges() != null) {
                builder.field(Role.INDICES.getPreferredName(), role.getIndicesPrivileges());
            }
            if (role.getMetadata() != null) {
                builder.field(Role.METADATA.getPreferredName(), role.getMetadata());
            }
            if (role.getRunAsPrivilege() != null) {
                builder.field(Role.RUN_AS.getPreferredName(), role.getRunAsPrivilege());
            }
            builder.endObject();
        }
        builder.endObject();
        if (metadata != null) {
            builder.field("metadata", metadata);
        }
        return builder.endObject();
    }

}
