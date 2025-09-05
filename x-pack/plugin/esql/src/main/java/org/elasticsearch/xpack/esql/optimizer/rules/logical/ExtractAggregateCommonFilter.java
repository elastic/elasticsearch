/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.extractCommon;

/**
 * Extract a per-function expression filter applied to all the aggs as a query {@link Filter}, when no groups are provided.
 * <p>
 *     Example:
 *     <pre>
 *         ... | STATS MIN(a) WHERE b > 0, MIN(c) WHERE b > 0 | ...
 *         =>
 *         ... | WHERE b > 0 | STATS MIN(a), MIN(c) | ...
 *     </pre>
 */
public final class ExtractAggregateCommonFilter extends OptimizerRules.OptimizerRule<Aggregate> {
    public ExtractAggregateCommonFilter() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate.groupings().isEmpty() == false) {
            return aggregate; // no optimization for grouped stats
        }

        // collect all filters from the agg functions
        List<Expression> filters = new ArrayList<>(aggregate.aggregates().size());
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFunction && aggFunction.hasFilter()) {
                filters.add(aggFunction.filter());
            } else {
                return aggregate; // (at least one) agg function has no filter -- skip optimization
            }
        }

        // extract common filters
        var common = extractCommon(filters);
        if (common.v1() == null) { // no common filter
            return aggregate;
        }

        // replace agg functions' filters with trimmed ones
        var newFilters = common.v2();
        List<NamedExpression> newAggs = new ArrayList<>(aggregate.aggregates().size());
        for (int i = 0; i < aggregate.aggregates().size(); i++) {
            var alias = (Alias) aggregate.aggregates().get(i);
            var newChild = ((AggregateFunction) alias.child()).withFilter(newFilters.get(i));
            newAggs.add(alias.replaceChild(newChild));
        }

        // build the new agg on top of extracted filter
        return aggregate.with(new Filter(aggregate.source(), aggregate.child(), common.v1()), aggregate.groupings(), newAggs);
    }
}
