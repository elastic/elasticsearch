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
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;

/**
 * Moves an EVAL holding a NESTED reduction below the LIMIT it follows, so a {@code SORT | LIMIT | EVAL NESTED_ANY(...)} reduces
 * the rows the limit keeps on the data node, which holds their child documents, instead of on the coordinator, which does not.
 * An EVAL keeps every row, so the limit is unchanged. It also moves below an MV_EXPAND whose expanded column it does not read:
 * each copy of a row has the same parent document, so it gets the same value.
 * <p>
 * {@link PushDownAndCombineLimits} keeps the limit above such an EVAL while planning the coordinator, and pushes it below on
 * the data node, so this runs only on the coordinator.
 */
public final class PushNestedReductionBelowLimit extends OptimizerRules.OptimizerRule<Eval> implements OptimizerRules.CoordinatorOnly {
    @Override
    protected LogicalPlan rule(Eval eval) {
        if (eval.fields().stream().noneMatch(f -> f.child().anyMatch(NestedReduction.class::isInstance))) {
            return eval;
        }
        if (eval.child() instanceof Limit limit) {
            return limit.replaceChild(eval.replaceChild(limit.child()));
        }
        if (eval.child() instanceof MvExpand expand && eval.references().contains(expand.expanded()) == false) {
            return expand.replaceChild(eval.replaceChild(expand.child()));
        }
        return eval;
    }
}
