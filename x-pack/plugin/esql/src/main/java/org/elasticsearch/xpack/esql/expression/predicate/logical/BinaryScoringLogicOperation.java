/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.xpack.esql.core.expression.predicate.PredicateBiFunction;

import java.util.function.BiFunction;

import static org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.SCORE_FOR_FALSE;

public enum BinaryScoringLogicOperation implements PredicateBiFunction<Double, Double, Double> {

    AND((l, r) -> {
        if (SCORE_FOR_FALSE == l || SCORE_FOR_FALSE == r) {
            return SCORE_FOR_FALSE;
        }
        if (l == null || r == null) {
            return null;
        }
        return l + r;
    }, "AND"),
    OR((l, r) -> {
        if (SCORE_FOR_FALSE == l || SCORE_FOR_FALSE == r) {
            return Math.max(l, r);
        }
        if (l == null || r == null) {
            return null;
        }
        return l + r;
    }, "OR");

    private final BiFunction<Double, Double, Double> process;
    private final String symbol;

    BinaryScoringLogicOperation(BiFunction<Double, Double, Double> process, String symbol) {
        this.process = process;
        this.symbol = symbol;
    }

    @Override
    public String symbol() {
        return symbol;
    }

    @Override
    public Double apply(Double left, Double right) {
        return process.apply(left, right);
    }

    @Override
    public final Double doApply(Double left, Double right) {
        return null;
    }

    @Override
    public String toString() {
        return symbol;
    }
}
