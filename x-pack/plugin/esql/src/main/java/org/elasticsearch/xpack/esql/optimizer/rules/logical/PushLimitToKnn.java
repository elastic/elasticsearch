/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;

/**
 * Traverses the logical plan and pushes down the limit to the KNN function(s) in filter expressions, so KNN can use
 * it to set k if not specified.
 *
 * <p>Each path is cut independently at a nested {@code Limit}, {@code TopN}, {@code TopNBy}, {@code LimitBy},
 * {@code Rerank}, or {@code Aggregate}. A {@code Limit} in one {@code UnionAll} branch therefore cannot suppress
 * {@code implicitK} on a {@code Knn} in a sibling branch.
 */
public class PushLimitToKnn extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {

    public PushLimitToKnn() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        return limit.transformDownSkipBranch((plan, skipBranch) -> {
            if (plan instanceof Filter filter) {
                Expression limitAppliedExpression = limitFilterExpressions(filter.condition(), limit, ctx);
                if (limitAppliedExpression.equals(filter.condition()) == false) {
                    return filter.with(limitAppliedExpression);
                }
            } else if (plan != limit
                && (plan instanceof Limit
                    || plan instanceof TopN
                    || plan instanceof TopNBy
                    || plan instanceof LimitBy
                    || plan instanceof Rerank
                    || plan instanceof Aggregate)) {
                        skipBranch.set(true);
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
                return knn.withImplicitK((Integer) limit.limit().fold(ctx.foldCtx()));
            }
            return exp;
        });
    }
}
