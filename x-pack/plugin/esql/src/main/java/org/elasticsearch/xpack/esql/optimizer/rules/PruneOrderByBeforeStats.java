/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public final class PruneOrderByBeforeStats extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate agg) {
        OrderBy order = findPullableOrderBy(agg.child());

        LogicalPlan p = agg;
        if (order != null) {
            p = agg.transformDown(OrderBy.class, o -> o == order ? order.child() : o);
        }
        return p;
    }

    private static OrderBy findPullableOrderBy(LogicalPlan plan) {
        OrderBy pullable = null;
        if (plan instanceof OrderBy o) {
            pullable = o;
        } else if (plan instanceof Eval
            || plan instanceof Filter
            || plan instanceof Project
            || plan instanceof RegexExtract
            || plan instanceof Enrich) {
                pullable = findPullableOrderBy(((UnaryPlan) plan).child());
            }
        return pullable;
    }

}
