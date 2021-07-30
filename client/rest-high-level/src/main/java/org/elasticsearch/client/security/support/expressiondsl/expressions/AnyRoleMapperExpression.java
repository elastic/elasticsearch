/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * An expression that evaluates to <code>true</code> if at least one of its children
 * evaluate to <code>true</code>.
 * An <em>any</em> expression with no children is never <code>true</code>.
 */
public final class AnyRoleMapperExpression extends CompositeRoleMapperExpression {

    private AnyRoleMapperExpression(String name, RoleMapperExpression[] elements) {
        super(name, elements);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<RoleMapperExpression> elements = new ArrayList<>();

        public Builder addExpression(final RoleMapperExpression expression) {
            assert expression != null : "expression cannot be null";
            elements.add(expression);
            return this;
        }

        public AnyRoleMapperExpression build() {
            return new AnyRoleMapperExpression(CompositeType.ANY.getName(), elements.toArray(new RoleMapperExpression[0]));
        }
    }
}
