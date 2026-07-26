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
        if (aggregate.groupings().size() > 0 || aggregate instanceof TimeSeriesAggregate) {
            return aggregate;
        }

        SearchStats stats = context.searchStats();
        LogicalPlan plan = aggregate;
        var aggs = aggregate.aggregates();
        Set<Expression> nonNullAggFields = Sets.newLinkedHashSetWithExpectedSize(aggs.size());
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        aggregate.child().forEachExpression(Alias.class, alias -> aliasesBuilder.put(alias.toAttribute(), alias.child()));
        AttributeMap<Expression> aliases = aliasesBuilder.build();
        for (var agg : aggs) {
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                // ignore literals (e.g. COUNT(1))
                // make sure the field exists at the source and is indexed (not runtime)

                if (af instanceof First || af instanceof Last) {
                    // Exceptionally allow this agg function to be passed down null values
                    return plan;
                }

                Set<Expression> fields = resolveNonNullAggFields(af.field(), aliases, stats);
                if (fields.isEmpty()) {
                    // otherwise bail out since unless disjunction needs to cover _all_ fields, things get filtered out
                    return plan;
                }
                nonNullAggFields.addAll(fields);
            }
        }

        if (nonNullAggFields.size() > 0) {
            Expression condition = Predicates.combineOr(
                nonNullAggFields.stream().map(f -> (Expression) new IsNotNull(aggregate.source(), f)).toList()
            );
            plan = aggregate.replaceChild(new Filter(aggregate.source(), aggregate.child(), condition));
        }
        return plan;
    }

    private static Set<Expression> resolveNonNullAggFields(Expression field, AttributeMap<Expression> aliases, SearchStats stats) {
        if (field.foldable()) {
            return Set.of();
        }

        if (field instanceof FieldAttribute fa && stats.isIndexed(fa.fieldName())) {
            return Set.of(field);
        }

        // Surrogate aggregations can introduce an alias around the original field, for example AVG(long)
        // becomes SUM(TO_DOUBLE(long)). Resolve that alias to keep the filter pushable to the source.
        Set<Expression> resolvedFields = new InferIsNotNull().resolveExpressionAsRootAttributes(field, aliases);
        if (resolvedFields.isEmpty()) {
            return Set.of();
        }

        for (Expression resolvedField : resolvedFields) {
            if (resolvedField instanceof FieldAttribute == false) {
                return Set.of();
            }
            FieldAttribute fa = (FieldAttribute) resolvedField;
            if (stats.isIndexed(fa.fieldName()) == false) {
                return Set.of();
            }
        }
        return resolvedFields;
    }
}
