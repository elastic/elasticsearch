/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.core.Nullable;
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
    private final List<TemplateRoleName> roleTemplates;
    private final RoleMapperExpression rules;

    private final Map<String, Object> metadata;
    private final RefreshPolicy refreshPolicy;

    @Deprecated
    public PutRoleMappingRequest(final String name, final boolean enabled, final List<String> roles, final RoleMapperExpression rules,
                                 @Nullable final Map<String, Object> metadata, @Nullable final RefreshPolicy refreshPolicy) {
        this(name, enabled, roles, Collections.emptyList(), rules, metadata, refreshPolicy);
    }

    public PutRoleMappingRequest(final String name, final boolean enabled, final List<String> roles, final List<TemplateRoleName> templates,
                                 final RoleMapperExpression rules, @Nullable final Map<String, Object> metadata,
                                 @Nullable final RefreshPolicy refreshPolicy) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("role-mapping name is missing");
        }
        this.name = name;
        this.enabled = enabled;
        this.roles = Collections.unmodifiableList(Objects.requireNonNull(roles, "role-mapping roles cannot be null"));
        this.roleTemplates = Collections.unmodifiableList(Objects.requireNonNull(templates, "role-mapping role_templates cannot be null"));
        if (this.roles.isEmpty() && this.roleTemplates.isEmpty()) {
            throw new IllegalArgumentException("in a role-mapping, one of roles or role_templates is required");
        }
        if (this.roles.isEmpty() == false && this.roleTemplates.isEmpty() == false) {
            throw new IllegalArgumentException("in a role-mapping, cannot specify both roles and role_templates");
        }
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

    public List<TemplateRoleName> getRoleTemplates() {
        return roleTemplates;
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
        return Objects.hash(name, enabled, refreshPolicy, roles, roleTemplates, rules, metadata);
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
            Objects.equals(roleTemplates, other.roleTemplates) &&
            Objects.equals(rules, other.rules) &&
            Objects.equals(metadata, other.metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("enabled", enabled);
        builder.field("roles", roles);
        builder.field("role_templates", roleTemplates);
        builder.field("rules", rules);
        builder.field("metadata", metadata);
        return builder.endObject();
    }
}
