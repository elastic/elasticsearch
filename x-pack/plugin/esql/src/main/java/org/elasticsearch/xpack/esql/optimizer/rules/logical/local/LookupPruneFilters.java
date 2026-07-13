/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneFilters;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery;

/**
 * Lookup-specific variant of {@link PruneFilters}. When a filter condition evaluates to {@code false}
 * and the filter's subtree contains a {@link ParameterizedQuery}, marks it as {@code emptyResult=true}
 * instead of collapsing the entire plan to a {@code LocalRelation}. This preserves the plan structure
 * so the {@code LookupExecutionPlanner} can still build the operator chain.
 */
public class LookupPruneFilters extends PruneFilters {

    @Override
    protected LogicalPlan handleAlwaysFalseFilter(Filter filter) {
        if (filter.anyMatch(n -> n instanceof ParameterizedQuery)) {
            return filter.child()
                .transformUp(
                    ParameterizedQuery.class,
                    pq -> new ParameterizedQuery(pq.source(), pq.output(), pq.matchFields(), pq.joinOnConditions(), true)
                );
        }
        return super.handleAlwaysFalseFilter(filter);
    }
}
