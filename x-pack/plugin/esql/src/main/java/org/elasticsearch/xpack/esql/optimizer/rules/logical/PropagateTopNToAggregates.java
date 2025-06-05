/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.Rule;

/**
 * Looks for the structure:
 * <pre>
 * TopN
 * \_Aggregate
 * </pre>
 * And replaces the Aggregate with an Aggregate with the TopN data.
 * (TODO: Create a new TopNAggregate node instead)
 */
public class PropagateTopNToAggregates extends Rule<TopN, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(
            TopN.class,
            this::applyRule
        );
    }

    private TopN applyRule(TopN topN) {
        // TODO: Handle TimeSeriesAggregate
        if (topN.child() instanceof Aggregate aggregate) {
            return topN.replaceChild(
                new Aggregate(
                    aggregate.source(),
                    aggregate.child(),
                    aggregate.groupings(),
                    aggregate.aggregates(),
                    topN.order(),
                    topN.limit()
                )
            );
        }
        return topN;
    }
}
