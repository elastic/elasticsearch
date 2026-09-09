/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pushes down any SORT + LIMIT (TopN) that appear immediately after a {@link MergePlan}
 * (a {@code FORK} or a leaf {@link UnionAll}), into the branches that have no pipeline breaker.
 * In the following example, assuming no FORK implicit limit is added, both FORK branches are missing a pipeline breaker:
 * {@snippet lang="esql" :
 * FROM my-index
 * | FORK (WHERE x) (WHERE y)
 * | SORT z
 * | LIMIT 10
 * }
 * By pushing down (TopN) in both branches, we reduce the number of rows that are returned to the coordinator.
 */
public class PushDownLimitAndOrderByIntoMergePlan extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {
    public PushDownLimitAndOrderByIntoMergePlan() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit, LogicalOptimizerContext context) {
        if (limit.child() instanceof OrderBy == false) {
            return limit;
        }

        OrderBy orderBy = (OrderBy) limit.child();
        if (orderBy.child() instanceof MergePlan == false) {
            return limit;
        }
        MergePlan mergePlan = (MergePlan) orderBy.child();
        // Allow TopN pushdown into a direct-leaf UnionAll (heterogeneous FROM shape).
        // Subquery-shape UnionAll branches are left alone: shouldPushDownPipelineBreakerIntoMergeBranch
        // returns false for them so the loop below would be a no-op anyway.
        if (mergePlan instanceof UnionAll unionAll && PushDownUtils.isLeafUnionAll(unionAll) == false) {
            return limit;
        }

        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;

        for (LogicalPlan child : mergePlan.children()) {
            LogicalPlan newChild = maybePushDownLimitAndOrderByToMergeBranch(limit, mergePlan, orderBy, child);
            changed = changed || newChild != child;
            newChildren.add(newChild);
        }

        return changed ? limit.replaceChild(orderBy.replaceChild(mergePlan.replaceChildren(newChildren))) : limit;
    }

    private LogicalPlan maybePushDownLimitAndOrderByToMergeBranch(Limit limit, MergePlan mergePlan, OrderBy orderBy, LogicalPlan child) {
        if (PushDownUtils.shouldPushDownPipelineBreakerIntoMergeBranch(child) == false) {
            return child;
        }

        Map<Expression, Expression> outputMap = PushDownUtils.outputMap(mergePlan, child);
        List<Order> orders = new ArrayList<>();
        for (Order order : orderBy.order()) {
            Expression orderExp = order.child().transformDown(exp -> {
                if (outputMap.containsKey(exp)) {
                    return outputMap.get(exp);
                }
                return exp;
            });

            orders.add(order.replaceChildren(List.of(orderExp)));
        }

        assert orderBy.order().size() == orders.size()
            : "Expected the same size for OrderBy but got " + orderBy.order().size() + "!=" + orders.size();

        return limit.replaceChild(new OrderBy(orderBy.source(), child, orders));
    }
}
