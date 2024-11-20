/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.predicate.PredicateBiFunction;

import java.util.function.BiFunction;

public enum BinaryComparisonOperation implements PredicateBiFunction<Object, Object, Boolean> {

    EQ(Comparisons::eq, "=="),
    NULLEQ(Comparisons::nulleq, "<=>"),
    NEQ(Comparisons::neq, "!="),
    GT(Comparisons::gt, ">"),
    GTE(Comparisons::gte, ">="),
    LT(Comparisons::lt, "<"),
    LTE(Comparisons::lte, "<=");

    private final BiFunction<Object, Object, Boolean> process;
    private final String symbol;

    BinaryComparisonOperation(BiFunction<Object, Object, Boolean> process, String symbol) {
        this.process = process;
        this.symbol = symbol;
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public Boolean apply(Object left, Object right) {
        if (this != NULLEQ && (left == null || right == null)) {
            return null;
        }
        return doApply(left, right);
    }

    @Override
    public final Boolean doApply(Object left, Object right) {
        return process.apply(left, right);
    }

    @Override
    public String toString() {
        return symbol;
    }
}
