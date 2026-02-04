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
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pushes down any SORT + LIMIT (TopN) that appear immediately after a FORK, into the FORK branches that have no pipeline breaker.
 * In the following example, assuming no FORK implicit limit is added, both FORK branches are missing a pipeline breaker:
 * <pre>{@code
 * FROM my-index
 * | FORK (WHERE x) (WHERE y)
 * | SORT z
 * | LIMIT 10
 * }</pre>
 * By pushing down (TopN) in both branches, we reduce the number of rows that are returned to the coordinator.
 */
public class PushDownLimitAndOrderByIntoFork extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext> {
    public PushDownLimitAndOrderByIntoFork() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Limit limit, LogicalOptimizerContext context) {
        // if the implicit limit is added to the FORK branches, we can do an early return
        // since we know the FORK branches will contain at least one pipeline breaker (the implicit LIMIT)
        // and we won't push down Limit + OrderBy into any branch
        if (context.configuration().pragmas().forkImplicitLimit()) {
            return limit;
        }

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

        assert orderBy.order().size() == orders.size()
            : "Expected the same size for OrderBy but got " + orderBy.order().size() + "!=" + orders.size();

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
        // We only push down when no pipeline breaker can be found, and we query an index.
        // If no EsRelation is found, we likely have a LocalRelation and we should definitely not push Limit and OrderBy
        // as they will be removed by other optimizations.
        Holder<Boolean> hasPipelineBreaker = new Holder<>(false);
        Holder<Boolean> hasEsRelation = new Holder<>(false);
        plan.forEachDown(p -> {
            if (p instanceof PipelineBreaker) {
                hasPipelineBreaker.set(true);
            }
            if (p instanceof EsRelation) {
                hasEsRelation.set(true);
            }
        });

        return hasEsRelation.get() && hasPipelineBreaker.get() == false;
    }
}
