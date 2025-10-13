/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;

/**
 * Traverses the logical plan and pushes down the limit to the KNN function(s) in filter expressions, so KNN can use
 * it to set k if not specified.
 */
public class PushLimitToEvalScore extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    final boolean local;

    public PushLimitToEvalScore(boolean local) {
        super(OptimizerRules.TransformDirection.DOWN);
        this.local = local;
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (false == local) {
            return limit;
        }
        Holder<Boolean> breakerReached = new Holder<>(false);
        Holder<Boolean> firstLimit = new Holder<>(false);
        return limit.transformDown(plan -> {
            if (breakerReached.get()) {
                // We reached a breaker and don't want to continue processing
                return plan;
            }
            if (plan instanceof Eval eval) {
                if (eval.fields().stream().anyMatch(x -> x.child() instanceof Score) && eval.child() instanceof Limit == false) {
                    return eval.replaceChild(limit.replaceChild(eval.child()));
                }
            }
            if (plan instanceof Limit) {
                // Break if it's not the initial limit
                breakerReached.set(firstLimit.get());
                firstLimit.set(true);
            } else if (plan instanceof TopN || plan instanceof Rerank || plan instanceof Aggregate) {
                breakerReached.set(true);
            }

            return plan;
        });
    }
}
