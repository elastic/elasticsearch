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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request object to create or update a role.
 */
public final class PutRoleRequest implements Validatable, ToXContentObject {

    private final Role role;
    private final RefreshPolicy refreshPolicy;

    public PutRoleRequest(Role role, @Nullable final RefreshPolicy refreshPolicy) {
        this.role = Objects.requireNonNull(role);
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public Role getRole() {
        return role;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(role, refreshPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PutRoleRequest other = (PutRoleRequest) obj;

        return (refreshPolicy == other.getRefreshPolicy()) &&
               Objects.equals(role, other.role);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
        return builder.endObject();
    }

}
