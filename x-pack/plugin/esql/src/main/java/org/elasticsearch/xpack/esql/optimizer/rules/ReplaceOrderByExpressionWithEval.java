/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.rawTemporaryName;

public final class ReplaceOrderByExpressionWithEval extends OptimizerRules.OptimizerRule<OrderBy> {
    private static int counter = 0;

    @Override
    protected LogicalPlan rule(OrderBy orderBy) {
        int size = orderBy.order().size();
        List<Alias> evals = new ArrayList<>(size);
        List<Order> newOrders = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var order = orderBy.order().get(i);
            if (order.child() instanceof Attribute == false) {
                var name = rawTemporaryName("order_by", String.valueOf(i), String.valueOf(counter++));
                var eval = new Alias(order.child().source(), name, order.child());
                newOrders.add(order.replaceChildren(List.of(eval.toAttribute())));
                evals.add(eval);
            } else {
                newOrders.add(order);
            }
        }
        if (evals.isEmpty()) {
            return orderBy;
        } else {
            var newOrderBy = new OrderBy(orderBy.source(), new Eval(orderBy.source(), orderBy.child(), evals), newOrders);
            return new Project(orderBy.source(), newOrderBy, orderBy.output());
        }
    }
}
