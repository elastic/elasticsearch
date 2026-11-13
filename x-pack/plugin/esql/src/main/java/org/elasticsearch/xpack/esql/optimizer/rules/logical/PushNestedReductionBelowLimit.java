/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Moves an EVAL holding a NESTED reduction below the LIMIT it follows, so a {@code SORT | LIMIT | EVAL NESTED_ANY(...)} reduces
 * the rows the limit keeps on the data node, where child values are read from the index, instead of on the coordinator, which
 * can only read {@code _source}. An EVAL keeps every row, so the limit is unchanged.
 * <p>
 * {@link PushDownAndCombineLimits} keeps the limit above such an EVAL while planning the coordinator, and pushes it below on
 * the data node, so this runs only on the coordinator.
 */
public final class PushNestedReductionBelowLimit extends OptimizerRules.OptimizerRule<Eval> implements OptimizerRules.CoordinatorOnly {
    @Override
    protected LogicalPlan rule(Eval eval) {
        if (eval.child() instanceof Limit limit
            && eval.fields().stream().anyMatch(f -> f.child().anyMatch(NestedReduction.class::isInstance))) {
            return limit.replaceChild(eval.replaceChild(limit.child()));
        }
        return eval;
    }
}
