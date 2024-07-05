/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

public final class PushDownAndCombineOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {
    @Override
    protected LogicalPlan rule(OrderBy orderBy) {
        LogicalPlan child = orderBy.child();

        if (child instanceof OrderBy childOrder) {
            // combine orders
            return new OrderBy(orderBy.source(), childOrder.child(), orderBy.order());
        } else if (child instanceof Project) {
            return LogicalPlanOptimizer.pushDownPastProject(orderBy);
        }

        return orderBy;
    }
}
