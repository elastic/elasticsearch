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
import org.elasticsearch.xpack.esql.plan.logical.TopN;

/**
 * Combines a Limit followed by a TopN into a single TopN.
 */
public final class CombineLimitTopN extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public CombineLimitTopN() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof TopN topn) {
            int thisLimitValue = (int) limit.limit().fold(ctx.foldCtx());
            int topNValue = (int) topn.limit().fold(ctx.foldCtx());
            if (topNValue <= thisLimitValue) {
                return topn;
            } else {
                return new TopN(topn.source(), topn.child(), topn.order(), limit.limit(), topn.isLocal());
            }
        }
        return limit;
    }
}
