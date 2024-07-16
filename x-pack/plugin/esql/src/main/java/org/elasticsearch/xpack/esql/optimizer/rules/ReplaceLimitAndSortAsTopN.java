/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

public final class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit plan) {
        LogicalPlan p = plan;
        if (plan.child() instanceof OrderBy o) {
            p = new TopN(plan.source(), o.child(), o.order(), plan.limit());
        }
        return p;
    }
}
