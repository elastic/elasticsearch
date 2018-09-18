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

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Expression of role mapper expressions which can be combined by operations like AND, OR
 * <p>
 * Expression builder example:
 * <pre>
 * {@code
 * final RoleMapperExpression allExpression = CompositeRoleMapperExpression.builder(CompositeType.ALL)
                .addExpression(CompositeRoleMapperExpression.builder(CompositeType.ANY)
                        .addExpression(FieldRoleMapperExpression.builder(FieldType.USERNAME)
                                .addValue("user1@example.org")
                                .build())
                        .addExpression(FieldRoleMapperExpression.builder(FieldType.USERNAME)
                                .addValue("user2@example.org")
                                .build())
                        .build())
                .addExpression(FieldRoleMapperExpression.builder(FieldType.METADATA)
                        .withKey("metadata.location")
                        .addValue("AMER")
                        .build())
                .build();
 * }
 * </pre>
 */
public class CompositeRoleMapperExpression implements RoleMapperExpression {
    private final String name;
    private final List<RoleMapperExpression> elements;

    CompositeRoleMapperExpression(final String name, final RoleMapperExpression... elements) {
        assert name != null : "field name cannot be null";
        assert elements != null : "at least one field expression is required";
        this.name = name;
        this.elements = Collections.unmodifiableList(Arrays.asList(elements));
    }

    public String getName() {
        return this.getName();
    }

    public List<RoleMapperExpression> getElements() {
        return elements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CompositeRoleMapperExpression that = (CompositeRoleMapperExpression) o;
        if (Objects.equals(this.getName(), that.getName()) == false) {
            return false;
        }
        return Objects.equals(this.getElements(), that.getElements());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, elements);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (CompositeType.EXCEPT.getName().equals(name)) {
            builder.field(name);
            builder.value(elements.get(0));
        } else {
            builder.startArray(name);
            for (RoleMapperExpression e : elements) {
                e.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    /**
     * Generates composite role mapper expression builder for give
     * {@link CompositeType}
     *
     * @param type {@link CompositeType} composite role expression types like
     * all, any etc.
     * @return {@link Builder} instance
     */
    public static Builder builder(CompositeType type) {
        return new Builder(type);
    }

    public enum CompositeType {
        ANY("any"), ALL("all"), EXCEPT("except");

        private static Map<String, CompositeType> nameToType = Collections.unmodifiableMap(initialize());
        private ParseField field;

        CompositeType(String name) {
            this.field = new ParseField(name);
        }

        public String getName() {
            return field.getPreferredName();
        }

        public ParseField getParseField() {
            return field;
        }

        public static CompositeType fromName(String name) {
            return nameToType.get(name);
        }

        private static Map<String, CompositeType> initialize() {
            Map<String, CompositeType> map = new HashMap<>();
            for (CompositeType field : values()) {
                map.put(field.getName(), field);
            }
            return map;
        }
    }

    public static final class Builder {
        private CompositeType field;
        private List<RoleMapperExpression> elements = new ArrayList<>();

        Builder(CompositeType field) {
            this.field = field;
        }

        public Builder addExpression(final RoleMapperExpression expression) {
            assert expression != null : "expression cannot be null";
            if (CompositeType.EXCEPT == field) {
                assert elements.isEmpty() : "except expression can have only one delegate expression";
            }
            elements.add(expression);
            return this;
        }

        public CompositeRoleMapperExpression build() {
            return new CompositeRoleMapperExpression(field.getName(), elements.toArray(new RoleMapperExpression[0]));
        }
    }
}

