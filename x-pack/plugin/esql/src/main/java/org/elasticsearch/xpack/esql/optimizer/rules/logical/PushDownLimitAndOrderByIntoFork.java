/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PushDownLimitAndOrderByIntoFork extends OptimizerRules.OptimizerRule<Limit> {
    public PushDownLimitAndOrderByIntoFork() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit) {
        if (limit.child() instanceof OrderBy == false) {
            return limit;
        }

        OrderBy orderBy = (OrderBy) limit.child();
        if (orderBy.child() instanceof Fork == false || orderBy.child() instanceof UnionAll) {
            return limit;
        }
        Fork fork = (Fork) orderBy.child();

        List<LogicalPlan> newForkChildren = new ArrayList<>();
        boolean changed = false;

        for (LogicalPlan forkChild : fork.children()) {
            LogicalPlan newForkChild = maybePushDownLimitAndOrderByToForkBranch(limit, fork, orderBy, forkChild);
            changed = changed || newForkChild != forkChild;
            newForkChildren.add(newForkChild);
        }

        return changed ? limit.replaceChild(orderBy.replaceChild(fork.replaceChildren(newForkChildren))) : limit;
    }

    private LogicalPlan maybePushDownLimitAndOrderByToForkBranch(Limit limit, Fork fork, OrderBy orderBy, LogicalPlan forkChild) {
        if (shouldPushDownIntoForkBranch(forkChild) == false) {
            return forkChild;
        }

        Map<Expression, Expression> outputMap = outputMap(fork, forkChild);
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

        return limit.replaceChild(new OrderBy(orderBy.source(), forkChild, orders));
    }

    private Map<Expression, Expression> outputMap(LogicalPlan plan, LogicalPlan otherPlan) {
        Map<Expression, Expression> outputMap = new HashMap<>();

        for (Attribute attr : plan.output()) {
            for (Attribute otherAttr : otherPlan.output()) {
                if (attr.name().equals(otherAttr.name())) {
                    outputMap.put(attr, otherAttr);
                }
            }
        }
        return outputMap;
    }

    private boolean shouldPushDownIntoForkBranch(LogicalPlan plan) {
        // We only push down when no pipeline breaker can be found
        Holder<Boolean> shouldPushDown = new Holder<>(false);
        plan.forEachDown(p -> {
            if (p instanceof PipelineBreaker) {
                shouldPushDown.set(true);
            }
            // this is pretty much a hack until we get an optimization to trim
            // empty FORK branches
            if (p instanceof LocalRelation) {
                shouldPushDown.set(true);
            }
        });

        return shouldPushDown.get() == false;
    }
}
