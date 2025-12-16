/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PushDownLimitAndOrderByIntoFork extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {
    public PushDownLimitAndOrderByIntoFork() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof OrderBy == false) {
            return limit;
        }

        OrderBy orderBy = (OrderBy) limit.child();
        if (orderBy.child() instanceof Fork == false || orderBy.child() instanceof UnionAll) {
            return limit;
        }
        Fork fork = (Fork) orderBy.child();

        var limitValue = (int) limit.limit().fold(ctx.foldCtx());
        List<LogicalPlan> newForkChildren = new ArrayList<>();
        boolean changed = false;

        for (LogicalPlan forkChild : fork.children()) {
            LogicalPlan newForkChild = maybePushDownLimitAndOrderByToForkBranch(limit, limitValue, fork, orderBy, forkChild, ctx);
            changed = changed || newForkChild != forkChild;
            newForkChildren.add(newForkChild);
        }

        return changed ? limit.replaceChild(orderBy.replaceChild(fork.replaceChildren(newForkChildren))) : limit;
    }

    private LogicalPlan maybePushDownLimitAndOrderByToForkBranch(
        Limit limit,
        int limitValue,
        Fork fork,
        OrderBy orderBy,
        LogicalPlan forkChild,
        LogicalOptimizerContext ctx
    ) {
        if (shouldPushDownIntoForkBranch(limitValue, forkChild, ctx) == false) {
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

    private boolean shouldPushDownIntoForkBranch(int limitValue, LogicalPlan forkChild, LogicalOptimizerContext ctx) {
        // figure out how we want to handle Joins/INLINE STATS
        if (forkChild instanceof UnaryPlan == false) {
            return false;
        }

        UnaryPlan plan = (UnaryPlan) forkChild;

        while (plan instanceof Aggregate == false) {
            if (plan instanceof Limit limit) {
                int otherLimit = (int) limit.limit().fold(ctx.foldCtx());
                return otherLimit > limitValue;
            } else if (plan instanceof MvExpand) {
                return true;
            }
            if (plan.child() instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan;
            } else {
                break;
            }
        }
        return false;
    }
}
