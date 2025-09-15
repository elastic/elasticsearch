/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Binary operator. Operators act as _special_ functions in that they have a symbol
 * instead of a name and do not use parentheses.
 * Further more they are not registered as the rest of the functions as are implicit
 * to the language.
 */
public abstract class BinaryPredicate<T, U, R, F extends PredicateBiFunction<T, U, R>> extends BinaryScalarFunction {

    private final F function;

    protected BinaryPredicate(Source source, Expression left, Expression right, F function) {
        super(source, left, right);
        this.function = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    public R fold(FoldContext ctx) {
        return function().apply((T) left().fold(ctx), (U) right().fold(ctx));
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), function.symbol());
    }

    @Override
    public boolean equals(Object obj) {
        // NB: the id and name are being ignored for binary expressions as most of them
        // are operators

        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryPredicate<?, ?, ?, ?> other = (BinaryPredicate<?, ?, ?, ?>) obj;

        return Objects.equals(symbol(), other.symbol()) && Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    public String symbol() {
        return function.symbol();
    }

    public F function() {
        return function;
    }

    @Override
    public String nodeString() {
        return left().nodeString() + " " + symbol() + " " + right().nodeString();
    }
}
