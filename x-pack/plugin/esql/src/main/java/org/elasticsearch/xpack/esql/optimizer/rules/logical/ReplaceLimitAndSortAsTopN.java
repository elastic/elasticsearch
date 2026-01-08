/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.GroupedTopN;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

public final class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit limit) {
        LogicalPlan p = limit;

        // TODO Is this correct for a constant? Would we have to introduce a new column with that constant name?
        if (limit.child() instanceof OrderBy o) {
            if (limit.groupKey() == null || limit.groupKey().foldable()) {
                p = new TopN(o.source(), o.child(), o.order(), limit.limit(), false);
            } else {
                p = new GroupedTopN(o.source(), o.child(), o.order(), limit.limit(), limit.groupKey(), false);
            }
        }
        return p;
    }
}
