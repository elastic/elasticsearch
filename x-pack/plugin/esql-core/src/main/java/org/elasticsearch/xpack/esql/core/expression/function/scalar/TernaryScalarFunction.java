/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.List;

public abstract class TernaryScalarFunction extends ScalarFunction {

    private final Expression left;
    private final Expression right;
    private final Expression arg;

    protected TernaryScalarFunction(Source source, Expression left, Expression right, Expression arg) {
        super(source, Arrays.asList(left, right, arg));
        this.left = left;
        this.right = right;
        this.arg = arg;
    }

    @Override
    public final TernaryScalarFunction replaceChildren(List<Expression> newChildren) {
        Expression newLeft = newChildren.get(0);
        Expression newRight = newChildren.get(1);
        Expression newArg = newChildren.get(2);

        return left.equals(newLeft) && right.equals(newRight) && arg.equals(newArg) ? this : replaceChildren(newLeft, newRight, newArg);
    }

    protected abstract TernaryScalarFunction replaceChildren(Expression newLeft, Expression newRight, Expression newArg);

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    public Expression arg() {
        return arg;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable() && arg.foldable();
    }
}
