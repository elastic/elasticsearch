/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Extract a per-function expression filter that is applied to all the aggs as a query {@link Filter}.
 * <p>
 *     Example:
 *     <pre>
 *         ... | STATS MIN(a) WHERE b > 0, MIN(c) WHERE b > 0 | ...
 *         =>
 *         ... | WHERE b > 0 | STATS MIN(a), MIN(c) | ...
 *     </pre>
 */
public final class ExtractStatsCommonFilter extends OptimizerRules.OptimizerRule<Aggregate> {
    public ExtractStatsCommonFilter() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        Expression filter = null;
        List<NamedExpression> newAggs = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias) {
                if (alias.child() instanceof AggregateFunction aggFunction && aggFunction.hasFilter()) {
                    if (filter == null) {
                        filter = aggFunction.filter();
                    } else if (aggFunction.filter().semanticEquals(filter) == false) {
                        return aggregate; // different filters -- skip optimization
                    }
                    // first or same filter -- remove it from the agg function
                    newAggs.add(alias.replaceChild(aggFunction.withFilter(Literal.TRUE)));
                } else {
                    return aggregate; // (at least one) agg function has no filter -- skip optimization
                }
            } else { // grouping
                newAggs.add(ne);
            }
        }
        if (filter == null) { // no agg function provided (STATS BY x)
            return aggregate;
        }
        return new Aggregate(
            aggregate.source(),
            new Filter(aggregate.source(), aggregate.child(), filter),
            aggregate.aggregateType(),
            aggregate.groupings(),
            newAggs
        );
    }
}
