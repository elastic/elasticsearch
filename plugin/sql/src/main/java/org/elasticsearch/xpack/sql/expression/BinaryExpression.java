/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class BinaryExpression extends Expression {

    private final Expression left, right;

    protected BinaryExpression(Location location, Expression left, Expression right) {
        super(location, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final BinaryExpression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }
    protected abstract BinaryExpression replaceChildren(Expression newLeft, Expression newRight);

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public boolean nullable() {
        return left.nullable() || left.nullable();
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        BinaryExpression other = (BinaryExpression) obj;
        return Objects.equals(left, other.left)
                && Objects.equals(right, other.right);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(left());
        sb.append(" ");
        sb.append(symbol());
        sb.append(" ");
        sb.append(right());
        return sb.toString();
    }

    public abstract String symbol();

    public abstract BinaryExpression swapLeftAndRight();
}
