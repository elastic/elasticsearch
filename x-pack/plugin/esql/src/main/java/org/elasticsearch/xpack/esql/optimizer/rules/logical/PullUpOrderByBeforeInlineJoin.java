/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SortAgnostic;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;

/**
 * Pulls "up" an {@link OrderBy} node that is not preceded by a {@link Limit}, but is preceded by an {@link InlineJoin}.
 * The InlineJoin is {@link SortAgnostic}, so the OrderBy can be pulled up without affecting the semantics of the join.
 * This is needed since otherwise the OrderBy would remain to be executed unbounded, which isn't supported.
 * If it's preceded by a {@link Limit}, it will be merged into a {@link org.elasticsearch.xpack.esql.plan.logical.TopN} later in the
 * "cleanup" optimization stage.
 */
public final class PullUpOrderByBeforeInlineJoin extends OptimizerRules.OptimizerRule<LogicalPlan> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        return plan.transformUp(LogicalPlan.class, PullUpOrderByBeforeInlineJoin::pullUpOrderByBeforeInlineJoin);
    }

    private static LogicalPlan pullUpOrderByBeforeInlineJoin(LogicalPlan plan) {
        if (plan instanceof InlineJoin inlineJoin) {
            OrderBy orderBy = findOrderByNotPrecededByLimit(inlineJoin);
            if (orderBy != null) {
                LogicalPlan newInlineJoin = inlineJoin.transformUp(OrderBy.class, ob -> ob == orderBy ? orderBy.child() : ob);
                return new OrderBy(orderBy.source(), newInlineJoin, orderBy.order());
            }
        }
        return plan;
    }

    // Finds an OrderBy node in the subtree of the provided plan that is not preceded by a Limit
    private static OrderBy findOrderByNotPrecededByLimit(LogicalPlan plan) {
        if (plan instanceof Limit) {
            return null;
        }
        if (plan instanceof OrderBy orderBy) {
            return orderBy;
        }
        for (LogicalPlan child : plan.children()) {
            if (child instanceof SortAgnostic) {
                OrderBy found = findOrderByNotPrecededByLimit(child);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }
}
