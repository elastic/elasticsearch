/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.Objects;

/**
 * True and null positions in a batch. True and null do not overlap. A position in neither mask is false only when that
 * position was inside the selection the evaluator was given. A position outside that selection was not evaluated, and
 * the false bit there is not a result.
 */
public final class PredicateResult {
    private final SelectionMask trues;
    private final SelectionMask nulls;

    private PredicateResult(SelectionMask trues, SelectionMask nulls) {
        if (trues.positionCount() != nulls.positionCount()) {
            throw new IllegalArgumentException("true and null masks differ in length");
        }
        if (trues.and(nulls).isEmpty() == false) {
            throw new IllegalArgumentException("a position cannot be both true and null");
        }
        this.trues = trues;
        this.nulls = nulls;
    }

    public static PredicateResult of(SelectionMask trues, SelectionMask nulls) {
        return new PredicateResult(trues, nulls);
    }

    public static PredicateResult none(int positionCount) {
        return new PredicateResult(SelectionMask.none(positionCount), SelectionMask.none(positionCount));
    }

    public SelectionMask trues() {
        return trues;
    }

    public SelectionMask nulls() {
        return nulls;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PredicateResult other && trues.equals(other.trues) && nulls.equals(other.nulls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trues, nulls);
    }

    @Override
    public String toString() {
        return "true " + trues + " null " + nulls;
    }
}
