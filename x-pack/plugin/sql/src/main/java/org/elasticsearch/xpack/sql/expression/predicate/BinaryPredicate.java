/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Objects;

/**
 * Binary operator. Operators act as _special_ functions in that they have a symbol
 * instead of a name and do not use parentheses.
 * Further more they are not registered as the rest of the functions as are implicit
 * to the language.
 */
public abstract class BinaryPredicate<T, U, R, F extends PredicateBiFunction<T, U, R>> extends BinaryScalarFunction {

    private final String name;
    private final F function;

    protected BinaryPredicate(Location location, Expression left, Expression right, F function) {
        super(location, left, right);
        this.name = name(left, right, function.symbol());
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

    @Override
    public String name() {
        return name;
    }

    public String symbol() {
        return function.symbol();
    }

    public F function() {
        return function;
    }

    private static String name(Expression left, Expression right, String symbol) {
        StringBuilder sb = new StringBuilder();
        sb.append(Expressions.name(left));
        if (!(left instanceof Literal)) {
            sb.insert(0, "(");
            sb.append(")");
        }
        sb.append(" ");
        sb.append(symbol);
        sb.append(" ");
        int pos = sb.length();
        sb.append(Expressions.name(right));
        if (!(right instanceof Literal)) {
            sb.insert(pos, "(");
            sb.append(")");
        }
        return sb.toString();
    }
}