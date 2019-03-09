/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request to create API key
 */
public final class CreateApiKeyRequest implements Validatable, ToXContentObject {

    private final String name;
    private final TimeValue expiration;
    private final List<Role> roles;
    private final RefreshPolicy refreshPolicy;

    /**
     * Create API Key request constructor
     * @param name name for the API key
     * @param roles list of {@link Role}s
     * @param expiration to specify expiration for the API key
     */
    public CreateApiKeyRequest(String name, List<Role> roles, @Nullable TimeValue expiration, @Nullable final RefreshPolicy refreshPolicy) {
        if (Strings.hasText(name)) {
            this.name = name;
        } else {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        this.roles = Objects.requireNonNull(roles, "roles may not be null");
        this.expiration = expiration;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
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

    @Override
    public int hashCode() {
        return Objects.hash(name, refreshPolicy, roles, expiration);
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
        return Objects.equals(name, that.name) && Objects.equals(refreshPolicy, that.refreshPolicy) && Objects.equals(roles, that.roles)
                && Objects.equals(expiration, that.expiration);
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
        return builder.endObject();
    }

}
