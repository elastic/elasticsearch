/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.List;

public final class ReplaceLimitAndSortAsTopN extends OptimizerRules.OptimizerRule<Limit> {

    @Override
    protected LogicalPlan rule(Limit limit) {
        LogicalPlan p = limit;

        if (limit.child() instanceof OrderBy o) {
            if (limit.groupings().stream().allMatch(Expression::foldable)) {
                p = new TopN(o.source(), o.child(), o.order(), limit.limit(), List.of(), false);
            } else {
                p = new TopN(o.source(), o.child(), o.order(), limit.limit(), limit.groupings(), false);
            }
        } else if (limit.groupings().isEmpty() == false) {
            throw new IllegalStateException("When PER is used in LIMIT, the query needs to have a SORT before the LIMIT");
        }

        return p;
    }
}
