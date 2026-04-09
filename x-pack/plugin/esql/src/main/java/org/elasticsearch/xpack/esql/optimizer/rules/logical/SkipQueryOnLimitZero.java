/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public final class SkipQueryOnLimitZero extends OptimizerRules.ParameterizedOptimizerRule<UnaryPlan, LogicalOptimizerContext> {
    public SkipQueryOnLimitZero() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan, LogicalOptimizerContext ctx) {
        if (plan instanceof Limit limit) {
            if (limit.limit().foldable() && Integer.valueOf(0).equals(limit.limit().fold(ctx.foldCtx()))) {
                return PruneEmptyPlans.skipPlan(limit);
            }
        } else if (plan instanceof LimitBy limitBy) {
            if (limitBy.limitPerGroup().foldable() && Integer.valueOf(0).equals(limitBy.limitPerGroup().fold(ctx.foldCtx()))) {
                return PruneEmptyPlans.skipPlan(limitBy);
            }
        }
        return plan;
    }
}
