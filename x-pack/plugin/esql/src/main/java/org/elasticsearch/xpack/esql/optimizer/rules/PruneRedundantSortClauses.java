/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.ExpressionSet;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

import java.util.ArrayList;

public final class PruneRedundantSortClauses extends OptimizerRules.OptimizerRule<OrderBy> {

    @Override
    protected LogicalPlan rule(OrderBy plan) {
        var referencedAttributes = new ExpressionSet<Order>();
        var order = new ArrayList<Order>();
        for (Order o : plan.order()) {
            if (referencedAttributes.add(o)) {
                order.add(o);
            }
        }

        return plan.order().size() == order.size() ? plan : new OrderBy(plan.source(), plan.child(), order);
    }
}
