/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.parser.RoleMapperExpressionParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A representation of a single role-mapping.
 *
 * @see RoleMapperExpression
 * @see RoleMapperExpressionParser
 */
public final class ExpressionRoleMapping {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ExpressionRoleMapping, String> PARSER = new ConstructingObjectParser<>("role-mapping", true,
        (args, name) -> new ExpressionRoleMapping(name, (RoleMapperExpression) args[0], (List<String>) args[1],
            (List<TemplateRoleName>) args[2], (Map<String, Object>) args[3], (boolean) args[4]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> RoleMapperExpressionParser.fromXContent(parser), Fields.RULES,
                ObjectParser.ValueType.OBJECT);
        PARSER.declareStringArray(optionalConstructorArg(), Fields.ROLES);
        PARSER.declareObjectArray(optionalConstructorArg(), (parser, ctx) -> TemplateRoleName.fromXContent(parser), Fields.ROLE_TEMPLATES);
        PARSER.declareField(constructorArg(), XContentParser::map, Fields.METADATA, ObjectParser.ValueType.OBJECT);
        PARSER.declareBoolean(constructorArg(), Fields.ENABLED);
    }

    private final String name;
    private final RoleMapperExpression expression;
    private final List<String> roles;
    private final List<TemplateRoleName> roleTemplates;
    private final Map<String, Object> metadata;
    private final boolean enabled;

    /**
     * Constructor for role mapping
     *
     * @param name role mapping name
     * @param expr {@link RoleMapperExpression} Expression used for role mapping
     * @param roles list of roles to be associated with the user
     * @param metadata metadata that helps to identify which roles are assigned
     * to the user
     * @param enabled a flag when {@code true} signifies the role mapping is active
     */
    public ExpressionRoleMapping(final String name, final RoleMapperExpression expr, final List<String> roles,
                                 final List<TemplateRoleName> templates, final Map<String, Object> metadata, boolean enabled) {
        this.name = name;
        this.expression = expr;
        this.roles = roles == null ? Collections.emptyList() : Collections.unmodifiableList(roles);
        this.roleTemplates = templates == null ? Collections.emptyList() : Collections.unmodifiableList(templates);
        this.metadata = (metadata == null) ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public RoleMapperExpression getExpression() {
        return expression;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<TemplateRoleName> getRoleTemplates() {
        return roleTemplates;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ExpressionRoleMapping that = (ExpressionRoleMapping) o;
        return this.enabled == that.enabled &&
            Objects.equals(this.name, that.name) &&
            Objects.equals(this.expression, that.expression) &&
            Objects.equals(this.roles, that.roles) &&
            Objects.equals(this.roleTemplates, that.roleTemplates) &&
            Objects.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression, roles, roleTemplates, metadata, enabled);
    }

    public interface Fields {
        ParseField ROLES = new ParseField("roles");
        ParseField ROLE_TEMPLATES = new ParseField("role_templates");
        ParseField ENABLED = new ParseField("enabled");
        ParseField RULES = new ParseField("rules");
        ParseField METADATA = new ParseField("metadata");
    }
}
