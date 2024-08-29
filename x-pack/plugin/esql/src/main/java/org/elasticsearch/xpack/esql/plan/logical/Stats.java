/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

/**
 * STATS-like operations. Like {@link Aggregate} and {@link InlineStats}.
 */
public interface Stats {
    /**
     * The user supplied text in the query for this command.
     */
    Source source();

    /**
     * Rebuild this plan with new groupings and new aggregates.
     */
    Stats with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates);

    /**
     * Have all the expressions in this plan been resolved?
     */
    boolean expressionsResolved();

    /**
     * The operation directly before this one in the plan.
     */
    LogicalPlan child();

    /**
     * List containing both the aggregate expressions and grouping expressions.
     */
    List<? extends NamedExpression> aggregates();

    /**
     * List containing just the grouping expressions.
     */
    List<Expression> groupings();

}
