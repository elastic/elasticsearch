/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.xpack.esql.core.expression.predicate.PredicateBiFunction;

import java.util.function.BiFunction;

import static org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator.NO_MATCH_SCORE;

public enum BinaryLogicOperation implements PredicateBiFunction<Boolean, Boolean, Boolean> {

    AND((l, r) -> {
        if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r)) {
            return Boolean.FALSE;
        }
        if (l == null || r == null) {
            return null;
        }
        return Boolean.logicalAnd(l.booleanValue(), r.booleanValue());
    }, (leftScore, rightScore) -> {
        if (NO_MATCH_SCORE == leftScore || NO_MATCH_SCORE == rightScore) {
            return NO_MATCH_SCORE;
        }
        return leftScore + rightScore;
    }, "AND"),
    OR((l, r) -> {
        if (Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r)) {
            return Boolean.TRUE;
        }
        if (l == null || r == null) {
            return null;
        }
        return Boolean.logicalOr(l.booleanValue(), r.booleanValue());
    }, (leftScore, rightScore) -> {
        if (NO_MATCH_SCORE == leftScore || NO_MATCH_SCORE == rightScore) {
            return Math.max(leftScore, rightScore);
        }
        return leftScore + rightScore;
    }, "OR");

    private final BiFunction<Boolean, Boolean, Boolean> process;
    private final BiFunction<Double, Double, Double> scoreFunction;
    private final String symbol;

    BinaryLogicOperation(BiFunction<Boolean, Boolean, Boolean> process, BiFunction<Double, Double, Double> scoreFunction, String symbol) {
        this.process = process;
        this.scoreFunction = scoreFunction;
        this.symbol = symbol;
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public Boolean apply(Boolean left, Boolean right) {
        return process.apply(left, right);
    }

    public Double score(Double leftScore, Double rightScore) {
        return scoreFunction.apply(leftScore, rightScore);
    }

    @Override
    public final Boolean doApply(Boolean left, Boolean right) {
        return null;
    }

    @Override
    public String toString() {
        return symbol;
    }
}
