/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.LocalSurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class LocalSubstituteSurrogateExpressions extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return context.searchStats() != null
            ? plan.transformUp(Eval.class, eval -> eval.transformExpressionsOnly(Function.class, f -> substitute(f, context.searchStats())))
            : plan;
    }

    /**
     * Perform the actual substitution.
     */
    private static Expression substitute(Expression e, SearchStats searchStats) {
        if (e instanceof LocalSurrogateExpression s) {
            Expression surrogate = s.surrogate(searchStats);
            if (surrogate != null) {
                return surrogate;
            }
        }
        return e;
    }

}
