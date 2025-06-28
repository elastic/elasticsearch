/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class SubstituteSurrogateExpressionsWithSearchStats extends ParameterizedRule<
    LogicalPlan,
    LogicalPlan,
    LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return plan.transformUp(
            Eval.class,
            eval -> eval.transformExpressionsOnly(Function.class, f -> substituteDateTruncBucketWithRoundTo(f, context.searchStats()))
        );
    }

    /**
     * Perform the actual substitution.
     */
    private static Expression substituteDateTruncBucketWithRoundTo(Expression e, SearchStats searchStats) {
        if (e instanceof SurrogateExpression s && searchStats != null) {
            Expression surrogate = s.surrogate(searchStats);
            if (surrogate != null) {
                return surrogate;
            }
        }
        return e;
    }

}
