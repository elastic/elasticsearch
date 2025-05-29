/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class SubstituteSurrogateExpressionsWithSearchStats extends OptimizerRules.ParameterizedOptimizerRule<
    LogicalPlan,
    LocalLogicalOptimizerContext> {
    public SubstituteSurrogateExpressionsWithSearchStats() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        return plan.transformExpressionsUp(DateTrunc.class, e -> rule(e, context.searchStats()));
    }

    /**
     * Perform the actual substitution.
     */
    public static Expression rule(Expression e, SearchStats searchStats) {
        if (e instanceof SurrogateExpression s && searchStats != null) {
            Expression surrogate = s.surrogate(searchStats);
            if (surrogate != null) {
                return surrogate;
            }
        }
        return e;
    }

}
