/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public class PushLimitToKnn extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushLimitToKnn() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        var currentLimit = (int) limit.limit().fold(ctx.foldCtx());
        Holder<Integer> currentLimitHolder = new Holder<>(currentLimit);
        return limit.transformDown(plan -> {
            if (plan instanceof Filter filter) {
                Expression limitAppliedExpression = limitFilterExpressions(filter.condition(), limit, ctx);
                if (limitAppliedExpression.equals(filter.condition()) == false) {
                    return filter.with(limitAppliedExpression);
                }
            } else if (plan instanceof Limit descendantLimit) {
                var newLimit = (int) descendantLimit.limit().fold(ctx.foldCtx());
                if (newLimit < currentLimitHolder.get()) {
                    currentLimitHolder.set(newLimit);
                }
                return descendantLimit;
            }

            return plan;
        });
    }

    /**
     * Applies a limit to the filter expressions of a condition. Some filter expressions, such as KNN function,
     * can be optimized by applying the limit directly to them.
     */
    private Expression limitFilterExpressions(Expression condition, Limit limit, LogicalOptimizerContext ctx) {
        return condition.transformDown(exp -> {
            if (exp instanceof Knn knn) {
                return knn.replaceLimit((int) limit.limit().fold(ctx.foldCtx()));
            }
            return exp;
        });
    }
}
