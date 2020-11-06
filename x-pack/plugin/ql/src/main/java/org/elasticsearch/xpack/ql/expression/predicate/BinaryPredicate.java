/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;

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
    public R fold() {
        return function().apply((T) left().fold(), (U) right().fold());
    }

    @Override
    protected String scriptMethodName() {
        return function.scriptMethodName();
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

        return Objects.equals(symbol(), other.symbol())
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
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