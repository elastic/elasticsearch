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

package org.elasticsearch.client.security.support.expressiondsl.fields;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
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
 * An expression that evaluates to <code>true</code> if a field (map element) matches
 * the provided values. A <em>field</em> expression may have more than one provided value, in which
 * case the expression is true if <em>any</em> of the values are matched.
 * <p>
 * Expression builder example:
 * <pre>
 * {@code
 * final RoleMapperExpression usernameExpression = FieldRoleMapperExpression.builder(FieldType.USERNAME)
                                .addValue("user1@example.org")
                                .build();
 * }
 * </pre>
 */
public class FieldRoleMapperExpression implements RoleMapperExpression {
    private final String NAME = "field";

    private final String field;
    private final List<Object> values;

    public FieldRoleMapperExpression(final String field, final Object... values) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("null or empty field name (" + field + ")");
        }
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("null or empty values (" + values + ")");
        }
        this.field = field;
        this.values = Collections.unmodifiableList(Arrays.asList(values));
    }

    public String getField() {
        return field;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final FieldRoleMapperExpression that = (FieldRoleMapperExpression) o;

        return Objects.equals(this.getField(), that.getField()) && Objects.equals(this.getValues(), that.getValues());
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        if (this.values.size() == 1) {
            builder.field(this.field);
            builder.value(values.get(0));
        } else {
            builder.startArray(this.field);
            for (Object value : values) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder.endObject();
    }

    /**
     * Generates field role mapper expression builder for give {@link FieldType}
     *
     * @param type {@link FieldType} composite role expression types like
     * username, groups etc.
     * @return {@link Builder} instance
     */
    public static Builder builder(FieldType type) {
        return new Builder(type);
    }

    public enum FieldType {
        FIELD("field"), USERNAME("username"), GROUPS("groups"), DN("dn"), METADATA("metadata.");

        private static Map<String, FieldType> nameToType = Collections.unmodifiableMap(initialize());
        private ParseField field;

        FieldType(String name) {
            this.field = new ParseField(name);
        }

        public String getName() {
            return field.getPreferredName();
        }

        public ParseField getParseField() {
            return field;
        }

        public static FieldType fromName(String name) {
            return nameToType.get(name);
        }

        private static Map<String, FieldType> initialize() {
            Map<String, FieldType> map = new HashMap<>();
            for (FieldType field : values()) {
                map.put(field.getName(), field);
            }
            return map;
        }
    }

    public static final class Builder {
        private FieldType field;
        private String key;
        private List<Object> elements = new ArrayList<>();

        Builder(FieldType field) {
            this.field = field;
        }

        public Builder withKey(String key) {
            assert Strings.hasLength(key) : "metadata key cannot be null or empty";
            assert FieldType.METADATA == field : "metadata key can only be provided when building MetadataFieldExpression";
            this.key = key;
            return this;
        }

        public Builder addValue(Object value) {
            elements.add(value);
            return this;
        }

        public FieldRoleMapperExpression build() {
            if (FieldType.METADATA == field) {
                String fieldName = key.startsWith(FieldType.METADATA.getName()) ? key : FieldType.METADATA.getName() + key;
                return new FieldRoleMapperExpression(fieldName, elements.toArray());
            } else {
                return new FieldRoleMapperExpression(field.getName(), elements.toArray());
            }
        }
    }
}
