/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Moves every sort key that is not an attribute into an {@link Eval} below the sort: the execution planner sorts by a channel.
 * {@link TopNBy} is covered too, since PromQL builds it directly rather than from an {@link OrderBy}.
 */
public final class ReplaceOrderByExpressionWithEval extends OptimizerRules.OptimizerRule<UnaryPlan> {
    private static int counter = 0;

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        if (plan instanceof OrderBy orderBy) {
            return replace(orderBy, orderBy.order(), (child, order) -> new OrderBy(orderBy.source(), child, order));
        }
        if (plan instanceof TopNBy topNBy) {
            return replace(
                topNBy,
                topNBy.order(),
                (child, order) -> new TopNBy(topNBy.source(), child, order, topNBy.limitPerGroup(), topNBy.groupings())
            );
        }
        return plan;
    }

    private static LogicalPlan replace(UnaryPlan sort, List<Order> orders, BiFunction<LogicalPlan, List<Order>, LogicalPlan> withOrder) {
        int size = orders.size();
        List<Alias> evals = new ArrayList<>(size);
        List<Order> newOrders = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var order = orders.get(i);
            if (order.child() instanceof Attribute == false) {
                var name = rawTemporaryName("order_by", String.valueOf(i), String.valueOf(counter++));
                var eval = new Alias(order.child().source(), name, order.child(), null, true);
                newOrders.add(order.replaceChildren(List.of(eval.toAttribute())));
                evals.add(eval);
            } else {
                newOrders.add(order);
            }
        }
        if (evals.isEmpty()) {
            return sort;
        } else {
            var newSort = withOrder.apply(new Eval(sort.source(), sort.child(), evals), newOrders);
            return new Project(sort.source(), newSort, sort.output());
        }
    }
}
