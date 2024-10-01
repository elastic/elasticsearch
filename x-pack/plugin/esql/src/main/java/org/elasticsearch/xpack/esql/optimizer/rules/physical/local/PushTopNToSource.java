/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class PushTopNToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = topNExec;
        PhysicalPlan child = topNExec.child();
        if (canPushSorts(child)
            && canPushDownOrders(topNExec.order(), x -> LucenePushDownUtils.hasIdenticalDelegate(x, ctx.searchStats()))) {
            var sorts = buildFieldSorts(topNExec.order());
            var limit = topNExec.limit();

            if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
                plan = exchangeExec.replaceChild(queryExec.withSorts(sorts).withLimit(limit));
            } else {
                plan = ((EsQueryExec) child).withSorts(sorts).withLimit(limit);
            }
        }
        return plan;
    }

    private static boolean canPushSorts(PhysicalPlan plan) {
        if (plan instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        if (plan instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        return false;
    }

    private boolean canPushDownOrders(List<Order> orders, Predicate<FieldAttribute> hasIdenticalDelegate) {
        // allow only exact FieldAttributes (no expressions) for sorting
        return orders.stream().allMatch(o -> LucenePushDownUtils.isPushableFieldAttribute(o.child(), hasIdenticalDelegate));
    }

    private List<EsQueryExec.FieldSort> buildFieldSorts(List<Order> orders) {
        List<EsQueryExec.FieldSort> sorts = new ArrayList<>(orders.size());
        for (Order o : orders) {
            sorts.add(new EsQueryExec.FieldSort(((FieldAttribute) o.child()).exactAttribute(), o.direction(), o.nullsPosition()));
        }
        return sorts;
    }
}
