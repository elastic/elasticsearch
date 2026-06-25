/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * Detects {@code TopNExec -> [EvalExec|ProjectExec]* -> AggregateExec[FINAL]} where the sort key
 * is an aggregate function output, and annotates the {@link AggregateExec} so the operator can
 * filter its selected group IDs to only the top-N at emit time.
 */
public class PushTopNToAggregation extends Rule<PhysicalPlan, PhysicalPlan> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        return plan.transformDown(TopNExec.class, topN -> {
            if (topN.order().size() != 1) {
                return topN;
            }
            AggregateExec agg = findAggregateExec(topN.child());
            if (agg == null || agg.groupings().isEmpty() || agg.getMode().isOutputPartial() || agg.topNSort() != null) {
                return topN;
            }
            Order order = topN.order().get(0);
            Expression sortKey = order.child();
            if (sortKey instanceof Attribute sortAttr && topN.limit() instanceof Literal limitLit) {
                int aggIndex = findAggregatorIndex(sortAttr, agg.aggregates());
                if (aggIndex >= 0) {
                    int limit = ((Number) limitLit.value()).intValue();
                    boolean asc = order.direction() == Order.OrderDirection.ASC;
                    AggregateExec.TopNSort topNSort = new AggregateExec.TopNSort(aggIndex, asc, limit);
                    return topN.transformDown(AggregateExec.class, a -> a == agg ? a.withTopNSort(topNSort) : a);
                }
            }
            return topN;
        });
    }

    private static AggregateExec findAggregateExec(PhysicalPlan plan) {
        PhysicalPlan current = plan;
        for (;;) {
            if (current instanceof EvalExec eval) {
                current = eval.child();
            } else if (current instanceof ProjectExec project) {
                current = project.child();
            } else {
                break;
            }
        }
        if (current instanceof AggregateExec agg && agg.getClass() == AggregateExec.class) {
            return agg;
        }
        return null;
    }

    private static int findAggregatorIndex(Attribute sortAttr, List<? extends NamedExpression> aggregates) {
        int aggIndex = 0;
        for (NamedExpression agg : aggregates) {
            Expression unwrapped = agg instanceof Alias alias ? alias.child() : agg;
            if (unwrapped instanceof AggregateFunction) {
                if (agg.toAttribute().id().equals(sortAttr.id())) {
                    return aggIndex;
                }
                aggIndex++;
            }
        }
        return -1;
    }
}
