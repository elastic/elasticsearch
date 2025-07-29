/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class SkipQueryOnLimitZero extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {
    public SkipQueryOnLimitZero() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.limit().foldable()) {
            if (Integer.valueOf(0).equals((limit.limit().fold(ctx.foldCtx())))) {
                return PruneEmptyPlans.skipPlan(limit);
            }
        }
        return limit;
    }
}
