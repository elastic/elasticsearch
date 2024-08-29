/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * This adds an explicit TopN node to a plan that only has an OrderBy right before Lucene.
 * To date, the only known use case that "needs" this is a query of the form
 * from test
 * | sort emp_no
 * | mv_expand first_name
 * | rename first_name AS x
 * | where x LIKE "*a*"
 * | limit 15
 * <p>
 * or
 * <p>
 * from test
 * | sort emp_no
 * | mv_expand first_name
 * | sort first_name
 * | limit 15
 * <p>
 * PushDownAndCombineLimits rule will copy the "limit 15" after "sort emp_no" if there is no filter on the expanded values
 * OR if there is no sort between "limit" and "mv_expand".
 * But, since this type of query has such a filter, the "sort emp_no" will have no limit when it reaches the current rule.
 */
public final class AddDefaultTopN extends LogicalPlanOptimizer.ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan, LogicalOptimizerContext context) {
        if (plan instanceof UnaryPlan unary && unary.child() instanceof OrderBy order && order.child() instanceof EsRelation relation) {
            var limit = new Literal(plan.source(), context.configuration().resultTruncationMaxSize(), DataType.INTEGER);
            return unary.replaceChild(new TopN(plan.source(), relation, order.order(), limit));
        }
        return plan;
    }
}
