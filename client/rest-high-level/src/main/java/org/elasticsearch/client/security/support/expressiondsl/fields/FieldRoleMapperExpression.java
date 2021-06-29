/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.fields;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * An expression that evaluates to <code>true</code> if a field (map element) matches
 * the provided values. A <em>field</em> expression may have more than one provided value, in which
 * case the expression is true if <em>any</em> of the values are matched.
 * <p>
 * Expression builder example:
 * <pre>
 * {@code
 * final RoleMapperExpression usernameExpression = FieldRoleMapperExpression.ofUsername("user1@example.org");
 * }
 * </pre>
 */
public class FieldRoleMapperExpression implements RoleMapperExpression {

    private final String field;
    private final List<Object> values;

    public FieldRoleMapperExpression(final String field, final Object... values) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("null or empty field name (" + field + ")");
        }
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("null or empty values for field (" + field + ")");
        }
        this.field = field;
        this.values = List.of(values);
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
        builder.startObject("field");
        builder.startArray(this.field);
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();
        builder.endObject();
        return builder.endObject();
    }

    public static FieldRoleMapperExpression ofUsername(Object... values) {
        return ofKeyValues("username", values);
    }

    public static FieldRoleMapperExpression ofGroups(Object... values) {
        return ofKeyValues("groups", values);
    }

    public static FieldRoleMapperExpression ofDN(Object... values) {
        return ofKeyValues("dn", values);
    }

    public static FieldRoleMapperExpression ofMetadata(String key, Object... values) {
        if (key.startsWith("metadata.") == false) {
            throw new IllegalArgumentException("metadata key must have prefix 'metadata.'");
        }
        return ofKeyValues(key, values);
    }

    public static FieldRoleMapperExpression ofKeyValues(String key, Object... values) {
        return new FieldRoleMapperExpression(key, values);
    }

}
