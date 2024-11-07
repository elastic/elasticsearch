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

import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.inCommon;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.subtract;

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
public final class ExtractStatsCommonFilter extends OptimizerRules.OptimizerRule<Aggregate> {
    public ExtractStatsCommonFilter() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate.groupings().isEmpty() == false) {
            return aggregate; // no optimization for grouped stats
        }
        
        // extract predicates common in all the filters
        List<Expression> commonFilters = null;
        @SuppressWarnings({ "rawtypes", "unchecked" })
        List<Expression>[] splitAnds = new List[aggregate.aggregates().size()];
        int i = 0;
        for (NamedExpression ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFunction && aggFunction.hasFilter()) {
                var split = splitAnd(aggFunction.filter());
                commonFilters = commonFilters == null ? split : inCommon(split, commonFilters);
                if (commonFilters.isEmpty()) {
                    return aggregate; // no common filter -- skip optimization
                }
                splitAnds[i++] = split;
            } else {
                return aggregate; // (at least one) agg function has no filter -- skip optimization
            }
        }

        // remove common filters from the agg functions
        List<NamedExpression> newAggs = new ArrayList<>(aggregate.aggregates().size());
        i = 0;
        for (NamedExpression ne : aggregate.aggregates()) {
            var alias = (Alias) ne;
            List<Expression> diffed = subtract(splitAnds[i++], commonFilters);
            Expression newFilter = diffed.isEmpty() ? Literal.TRUE : combineAnd(diffed);
            newAggs.add(alias.replaceChild(((AggregateFunction) alias.child()).withFilter(newFilter)));
        }

        // build the new agg on top of extracted filter
        return new Aggregate(
            aggregate.source(),
            new Filter(aggregate.source(), aggregate.child(), combineAnd(commonFilters)),
            aggregate.aggregateType(),
            aggregate.groupings(),
            newAggs
        );
    }
}
