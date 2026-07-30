/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Collection;
import java.util.Set;

/**
 * The vast majority of aggs ignore null entries - this rule adds a pushable filter, as it is cheap
 * to execute, to filter these entries out to begin with.
 * STATS x = min(a), y = sum(b)
 * becomes
 * | WHERE a IS NOT NULL OR b IS NOT NULL
 * | STATS x = min(a), y = sum(b)
 * <br>
 * Unfortunately this optimization cannot be applied when grouping is necessary since it can filter out
 * groups containing only null values
 */
public class InferNonNullAggConstraint extends OptimizerRules.ParameterizedOptimizerRule<Aggregate, LocalLogicalOptimizerContext> {
    public InferNonNullAggConstraint() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate, LocalLogicalOptimizerContext context) {
        // only look at aggregates with default grouping
        if (aggregate.aggregates().isEmpty() || aggregate.groupings().isEmpty() == false || aggregate instanceof TimeSeriesAggregate) {
            return aggregate;
        }

        SearchStats stats = context.searchStats();

        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        aggregate.forEachUp(p -> p.forEachExpression(Alias.class, a -> aliasesBuilder.put(a.toAttribute(), a.child())));
        AttributeMap<Expression> aliases = aliasesBuilder.build();

        var aggs = aggregate.aggregates();
        Set<Expression> predicates = Sets.newLinkedHashSetWithExpectedSize(aggs.size());
        for (var agg : aggs) {
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                if (af instanceof First || af instanceof Last) {
                    // First (Last) may return null if that's first (last) value, so needs nulls.
                    // TODO: this blocklist is a picking timebomb. Create marker interface on agg.fns
                    // `IgnoresNulls` and take it from there.
                    return aggregate;
                }
                Expression field = af.field();
                if (field.foldable()) {
                    // Ignore literals (e.g. COUNT(1))
                    return aggregate;
                }
                Collection<Expression> attributes = InferIsNotNull.resolveExpressionAsRootAttributes(field, aliases, aggregate.inputSet());
                // make sure the field exists at the source and is indexed (not runtime)
                attributes = attributes.stream().filter(a -> a instanceof FieldAttribute fa && stats.isIndexed(fa.fieldName())).toList();
                if (attributes.isEmpty()) {
                    // bail out, because all rows are needed for this aggregation and no filter can be added
                    return aggregate;
                }

                // All attributes returned by `InferIsNotNull.resolveExpressionAsRootAttributes`
                // must be non-null, otherwise the aggregation function receives null from this
                // row, which is ignored.
                // This is needed for surrogates like: AVG(x) = SUM(TO_DOUBLE(x)) / COUNT(x).
                predicates.add(Predicates.combineAnd(attributes.stream().map(a -> new IsNotNull(aggregate.source(), a)).toList()));
            }
        }

        // If all predicates are false, a document contributes to no aggregation at all.
        // Hence, we can add a filter by an "or" of all predicates.
        return aggregate.replaceChild(new Filter(aggregate.source(), aggregate.child(), Predicates.combineOr(predicates)));
    }
}
