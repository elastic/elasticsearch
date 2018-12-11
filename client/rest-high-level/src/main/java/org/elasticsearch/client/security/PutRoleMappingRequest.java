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
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Request object to create or update a role mapping.
 */
public final class PutRoleMappingRequest implements Validatable, ToXContentObject {

    private final String name;
    private final boolean enabled;
    private final List<String> roles;
    private final RoleMapperExpression rules;

    private final Map<String, Object> metadata;
    private final RefreshPolicy refreshPolicy;

    public PutRoleMappingRequest(final String name, final boolean enabled, final List<String> roles, final RoleMapperExpression rules,
            @Nullable final Map<String, Object> metadata, @Nullable final RefreshPolicy refreshPolicy) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("role-mapping name is missing");
        }
        this.name = name;
        this.enabled = enabled;
        if (roles == null || roles.isEmpty()) {
            throw new IllegalArgumentException("role-mapping roles are missing");
        }
        this.roles = Collections.unmodifiableList(roles);
        this.rules = Objects.requireNonNull(rules, "role-mapping rules are missing");
        this.metadata = (metadata == null) ? Collections.emptyMap() : metadata;
        this.refreshPolicy = (refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy;
    }

    public String getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public List<String> getRoles() {
        return roles;
    }

    public RoleMapperExpression getRules() {
        return rules;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, enabled, refreshPolicy, roles, rules, metadata);
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
        final PutRoleMappingRequest other = (PutRoleMappingRequest) obj;

        return (enabled == other.enabled) &&
               (refreshPolicy == other.refreshPolicy) &&
               Objects.equals(name, other.name) &&
               Objects.equals(roles, other.roles) &&
               Objects.equals(rules, other.rules) &&
               Objects.equals(metadata, other.metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("enabled", enabled);
        builder.field("roles", roles);
        builder.field("rules", rules);
        builder.field("metadata", metadata);
        return builder.endObject();
    }

}
