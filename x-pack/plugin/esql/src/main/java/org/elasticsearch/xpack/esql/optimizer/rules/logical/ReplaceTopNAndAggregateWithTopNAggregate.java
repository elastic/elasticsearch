/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

/**
 * Looks for:
 * <pre>
 * {@link TopN}
 * \_{@link Aggregate}
 * </pre>
 * And replaces it with a {@link TopNAggregate}.
 * <p>
 *     {@link TimeSeriesAggregate} subclass should not appear here after a {@link TopN}. See {@link TranslateTimeSeriesAggregate}.
 * </p>
 */
public class ReplaceTopNAndAggregateWithTopNAggregate extends Rule<TopN, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(TopN.class, this::applyRule);
    }

    private LogicalPlan applyRule(TopN topN) {
        if (topN.child() instanceof Aggregate aggregate && aggregate instanceof TopNAggregate == false) {
            assert aggregate.getClass() == Aggregate.class : "Only Aggregate can be replaced with TopNAggregate";

            return new TopNAggregate(
                aggregate.source(),
                aggregate.child(),
                aggregate.groupings(),
                aggregate.aggregates(),
                topN.order(),
                topN.limit()
            );
        }
        return topN;
    }
}
