/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNAggregate;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

/**
 * Break TopNAggregate back into TopN + Aggregate to allow the order rules to kick in.
 */
public class ReplaceTopNAggregateWithTopNAndAggregate extends OptimizerRules.OptimizerRule<TopNAggregate> {
    public ReplaceTopNAggregateWithTopNAndAggregate() {
        super(UP);
    }

    @Override
    protected LogicalPlan rule(TopNAggregate plan) {
        return new TopN(
            plan.source(),
            new Aggregate(plan.source(), plan.child(), plan.groupings(), plan.aggregates()),
            plan.order(),
            plan.limit()
        );
    }
}
