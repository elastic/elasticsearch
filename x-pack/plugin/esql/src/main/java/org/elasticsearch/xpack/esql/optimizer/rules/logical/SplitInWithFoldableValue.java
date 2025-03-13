/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 3 in (field, 4, 5) --> 3 in (field) or 3 in (4, 5)
 */
public final class SplitInWithFoldableValue extends OptimizerRules.OptimizerExpressionRule<In> {

    public SplitInWithFoldableValue() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    public Expression rule(In in, LogicalOptimizerContext ctx) {
        if (in.value().foldable()) {
            List<Expression> foldables = new ArrayList<>(in.list().size());
            List<Expression> nonFoldables = new ArrayList<>(in.list().size());
            in.list().forEach(e -> {
                if (e.foldable() && Expressions.isGuaranteedNull(e) == false) { // keep `null`s, needed for the 3VL
                    foldables.add(e);
                } else {
                    nonFoldables.add(e);
                }
            });
            if (foldables.isEmpty() == false && nonFoldables.isEmpty() == false) {
                In withFoldables = new In(in.source(), in.value(), foldables);
                In withoutFoldables = new In(in.source(), in.value(), nonFoldables);
                return new Or(in.source(), withFoldables, withoutFoldables);
            }
        }
        return in;
    }
}
