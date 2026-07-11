/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

/**
 * Standard six-way comparison operators for {@link SqlPredicate.Comparison}.
 * <p>
 * {@link #commute()} flips the operator when the ESQL comparison arrives with the literal on the left
 * (e.g. {@code 10 < salary} -> {@code salary > 10}); the translator always rewrites comparisons to
 * column-on-the-left form, so {@link SqlPredicate.Comparison} never needs a "literal-on-left" variant.
 */
public enum CompOp {
    EQ("="),
    NEQ("<>"),
    LT("<"),
    LTE("<="),
    GT(">"),
    GTE(">=");

    private final String symbol;

    CompOp(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    /**
     * Returns the operator that produces the same result when its operands are swapped.
     * {@code x < y} becomes {@code y > x}, etc. {@code EQ}/{@code NEQ} are self-symmetric.
     */
    public CompOp commute() {
        return switch (this) {
            case EQ -> EQ;
            case NEQ -> NEQ;
            case LT -> GT;
            case LTE -> GTE;
            case GT -> LT;
            case GTE -> LTE;
        };
    }
}
