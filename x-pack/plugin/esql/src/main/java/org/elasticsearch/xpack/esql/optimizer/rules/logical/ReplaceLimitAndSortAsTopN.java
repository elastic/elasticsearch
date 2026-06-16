/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.ChangePointFillEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public final class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<UnaryPlan> {

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        LogicalPlan p = plan;
        if (plan instanceof Limit limit) {
            if (plan.child() instanceof OrderBy o) {
                p = new TopN(o.source(), o.child(), o.order(), limit.limit(), false);
            } else if (plan.child() instanceof ChangePointFillEmptyBuckets fill && fill.child() instanceof OrderBy o) {
                p = new ChangePointFillEmptyBuckets(
                    fill.source(),
                    new TopN(o.source(), o.child(), o.order(), limit.limit(), false),
                    fill.value(),
                    fill.key(),
                    fill.groupings(),
                    fill.dateBucketRounding(),
                    fill.minDate(),
                    fill.maxDate()
                );
            }
        } else if (plan instanceof LimitBy limitBy) {
            if (plan.child() instanceof OrderBy o) {
                p = new TopNBy(o.source(), o.child(), o.order(), limitBy.limitPerGroup(), limitBy.groupings());
            } else if (plan.child() instanceof ChangePointFillEmptyBuckets fill && fill.child() instanceof OrderBy o) {
                p = new ChangePointFillEmptyBuckets(
                    fill.source(),
                    new TopNBy(o.source(), o.child(), o.order(), limitBy.limitPerGroup(), limitBy.groupings()),
                    fill.value(),
                    fill.key(),
                    fill.groupings(),
                    fill.dateBucketRounding(),
                    fill.minDate(),
                    fill.maxDate()
                );
            }
        }
        return p;
    }
}
