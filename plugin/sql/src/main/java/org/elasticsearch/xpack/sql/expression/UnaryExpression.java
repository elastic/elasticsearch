/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

import static java.util.Collections.singletonList;

import java.util.List;

public abstract class UnaryExpression extends Expression {

    private final Expression child;

    protected UnaryExpression(Location location, Expression child) {
        super(location, singletonList(child));
        this.child = child;
    }

    @Override
    public final UnaryExpression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return replaceChild(newChildren.get(0));
    }
    protected abstract UnaryExpression replaceChild(Expression newChild);

    public Expression child() {
        return child;
    }

    @Override
    public boolean foldable() {
        return child.foldable();
    }

    @Override
    public boolean nullable() {
        return child.nullable();
    }

    @Override
    public boolean resolved() {
        return child.resolved();
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(child);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnaryExpression other = (UnaryExpression) obj;
        return Objects.equals(child, other.child);
    }
}
