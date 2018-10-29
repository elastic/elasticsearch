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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

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
                    (Map<String, Object>) args[2], (boolean) args[3]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> RoleMapperExpressionParser.fromXContent(parser), Fields.RULES,
                ObjectParser.ValueType.OBJECT);
        PARSER.declareStringArray(constructorArg(), Fields.ROLES);
        PARSER.declareField(constructorArg(), XContentParser::map, Fields.METADATA, ObjectParser.ValueType.OBJECT);
        PARSER.declareBoolean(constructorArg(), Fields.ENABLED);
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

    public interface Fields {
        ParseField ROLES = new ParseField("roles");
        ParseField ENABLED = new ParseField("enabled");
        ParseField RULES = new ParseField("rules");
        ParseField METADATA = new ParseField("metadata");
    }
}
