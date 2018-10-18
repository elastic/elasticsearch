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

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.parser.RoleMapperExpressionParser;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A representation of a single role-mapping.
 *
 * @see RoleMapperExpression
 * @see RoleMapperExpressionParser
 */
public final class ExpressionRoleMapping {

    static final ObjectParser<Builder, String> PARSER = new ObjectParser<>("role-mapping", Builder::new);

    static {
        PARSER.declareStringArray(Builder::roles, Fields.ROLES);
        PARSER.declareField(Builder::rules, RoleMapperExpressionParser::parseObject, Fields.RULES, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(Builder::metadata, XContentParser::map, Fields.METADATA, ObjectParser.ValueType.OBJECT);
        PARSER.declareBoolean(Builder::enabled, Fields.ENABLED);
    }

    private final String name;
    private final RoleMapperExpression expression;
    private final List<String> roles;
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
            final Map<String, Object> metadata, boolean enabled) {
        this.name = name;
        this.expression = expr;
        this.roles = Collections.unmodifiableList(roles);
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

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (enabled ? 1231 : 1237);
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((metadata == null) ? 0 : metadata.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((roles == null) ? 0 : roles.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final ExpressionRoleMapping other = (ExpressionRoleMapping) obj;
        if (enabled != other.enabled)
            return false;
        if (expression == null) {
            if (other.expression != null)
                return false;
        } else if (!expression.equals(other.expression))
            return false;
        if (metadata == null) {
            if (other.metadata != null)
                return false;
        } else if (!metadata.equals(other.metadata))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (roles == null) {
            if (other.roles != null)
                return false;
        } else if (!roles.equals(other.roles))
            return false;
        return true;
    }

    /**
     * Used to facilitate the use of {@link ObjectParser} (via {@link #PARSER}).
     */
    static class Builder {
        private RoleMapperExpression rules;
        private List<String> roles;
        private Map<String, Object> metadata = Collections.emptyMap();
        private Boolean enabled;

        Builder rules(RoleMapperExpression expression) {
            this.rules = expression;
            return this;
        }

        Builder roles(List<String> roles) {
            this.roles = roles;
            return this;
        }

        Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        ExpressionRoleMapping build(String name) {
            if (roles == null) {
                throw missingField(name, Fields.ROLES);
            }
            if (rules == null) {
                throw missingField(name, Fields.RULES);
            }
            if (enabled == null) {
                throw missingField(name, Fields.ENABLED);
            }
            return new ExpressionRoleMapping(name, rules, roles, metadata, enabled);
        }

        private IllegalStateException missingField(String id, ParseField field) {
            return new IllegalStateException("failed to parse role-mapping [" + id + "]. missing field [" + field + "]");
        }

    }

    public interface Fields {
        ParseField ROLES = new ParseField("roles");
        ParseField ENABLED = new ParseField("enabled");
        ParseField RULES = new ParseField("rules");
        ParseField METADATA = new ParseField("metadata");
    }
}
