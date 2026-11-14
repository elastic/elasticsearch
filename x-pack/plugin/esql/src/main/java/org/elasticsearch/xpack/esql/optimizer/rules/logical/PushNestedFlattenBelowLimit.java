/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

/**
 * Moves NESTED_FLATTEN below the LIMIT and SORT it follows. It reads the objects on the data node that holds the parent, so it
 * cannot run after a LIMIT, which runs on the coordinator. It keeps every row, its order and its parent columns, so neither
 * the limit nor the sort changes.
 * <p>
 * {@link PushDownAndCombineLimits} then leaves a local copy of the limit below it. A local limit already runs on the data node,
 * so it stays where it is, which keeps this from moving it again.
 */
public final class PushNestedFlattenBelowLimit extends OptimizerRules.OptimizerRule<NestedExpand>
    implements
        OptimizerRules.CoordinatorOnly {
    @Override
    protected LogicalPlan rule(NestedExpand flatten) {
        if (flatten.flatten() == false) {
            return flatten;
        }
        if (flatten.child() instanceof Limit limit && limit.local() == false) {
            return limit.replaceChild(flatten.replaceChild(limit.child()));
        }
        if (flatten.child() instanceof OrderBy orderBy) {
            return orderBy.replaceChild(flatten.replaceChild(orderBy.child()));
        }
        return flatten;
    }
}
