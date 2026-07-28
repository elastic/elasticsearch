/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

/**
 * Marks every node in the optimized plan as fully optimized.
 *
 * <p>Note: {@link org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer#optimize(LogicalPlan)}
 * unconditionally calls {@link LogicalPlan#setOptimized()} on the result of {@code execute()} regardless of
 * whether this rule ran, so disabling this rule does not cause the {@code "Expected optimized plan"}
 * {@link IllegalStateException} that one might expect. The rule is kept for clarity but is effectively
 * redundant from a correctness standpoint.</p>
 */
public final class SetAsOptimized extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        plan.forEachUp(SetAsOptimized::rule);
        return plan;
    }

    private static void rule(LogicalPlan plan) {
        if (plan.optimized() == false) {
            plan.setOptimized();
        }
    }
}
