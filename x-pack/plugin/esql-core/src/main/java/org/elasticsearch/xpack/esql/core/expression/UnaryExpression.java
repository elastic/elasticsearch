/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public abstract class UnaryExpression extends Expression {

    private final Expression child;

    protected UnaryExpression(Source source, Expression child) {
        super(source, singletonList(child));
        this.child = child;
    }

    @Override
    public final UnaryExpression replaceChildren(List<Expression> newChildren) {
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
    public Nullability nullable() {
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
