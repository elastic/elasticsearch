/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

import java.util.ArrayList;
import java.util.List;

public final class PruneLiteralsInOrderBy extends OptimizerRules.OptimizerRule<OrderBy> {

    @Override
    protected LogicalPlan rule(OrderBy ob) {
        List<Order> prunedOrders = new ArrayList<>();

        for (Order o : ob.order()) {
            if (o.child().foldable()) {
                prunedOrders.add(o);
            }
        }

        // everything was eliminated, the order isn't needed anymore
        if (prunedOrders.size() == ob.order().size()) {
            return ob.child();
        }
        if (prunedOrders.size() > 0) {
            List<Order> newOrders = new ArrayList<>(ob.order());
            newOrders.removeAll(prunedOrders);
            return new OrderBy(ob.source(), ob.child(), newOrders);
        }

        return ob;
    }
}
