/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Expression of role mapper expressions which can be combined by operators like AND, OR
 * <p>
 * Expression builder example:
 * <pre>
 * {@code
 * final RoleMapperExpression allExpression = AllRoleMapperExpression.builder()
                    .addExpression(AnyRoleMapperExpression.builder()
                            .addExpression(FieldRoleMapperExpression.ofUsername("user1@example.org"))
                            .addExpression(FieldRoleMapperExpression.ofUsername("user2@example.org"))
                            .build())
                    .addExpression(FieldRoleMapperExpression.ofMetadata("metadata.location", "AMER"))
                    .addExpression(new ExceptRoleMapperExpression(FieldRoleMapperExpression.ofUsername("user3@example.org")))
                    .build();
 * }
 * </pre>
 */
public abstract class CompositeRoleMapperExpression implements RoleMapperExpression {
    private final String name;
    private final List<RoleMapperExpression> elements;

    CompositeRoleMapperExpression(final String name, final RoleMapperExpression... elements) {
        assert name != null : "field name cannot be null";
        assert elements != null : "at least one field expression is required";
        this.name = name;
        this.elements = List.of(elements);
    }

    public String getName() {
        return this.name;
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
        builder.startArray(name);
        for (RoleMapperExpression e : elements) {
            e.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

}

