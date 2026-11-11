/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <p><strong>NOTE: THIS RULE IS INTENDED TO BE RUN ONLY ONCE. DO NOT INCLUDE IT IN MULTI-RUN BATCHES.</strong> The expectation is that
 * this will be run as part of the {@link org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer} in the local rewrite batch</p>
 * <p>
 * TSDB often ends up storing many null values for metrics columns (since not every time series contains every metric).  However, loading
 * many null values can negatively impact query performance.  To reduce that, this rule applies filters to remove null values on all
 * metrics involved in the query.  In the case that there are multiple metrics, the not null checks are OR'd together, so we accept rows
 * where any of the metrics have values.
 * </p>
 * <p>
 * Aggregation inputs computed in an {@code EVAL} are traced back through their aliases to the fields they read, as long as every
 * expression on the way is {@link AnyNullIsNull}, like casts and arithmetic. Such an input is null whenever one of its root metrics is
 * null, so filtering on the root metric fields only drops rows the aggregation would see as null anyway. Filtering on the root fields
 * rather than on the computed values also lets the filter be pushed down to Lucene as {@code exists} queries.
 * </p>
 */
public final class IgnoreNullMetrics extends OptimizerRules.OptimizerRule<TimeSeriesAggregate> {
    @Override
    public LogicalPlan rule(TimeSeriesAggregate agg) {
        AttributeMap<Expression> aliases = aliases(agg.child());
        // Preserve expression order so equivalent plans produce the same null-filter predicate.
        Set<Attribute> metrics = new LinkedHashSet<>();
        agg.forEachExpression(Attribute.class, attr -> {
            Set<Attribute> roots = new LinkedHashSet<>();
            if (resolveRoots(attr, aliases, roots)) {
                for (Attribute root : roots) {
                    if (root.isMetric()) {
                        metrics.add(root);
                    }
                }
            }
        });
        if (metrics.isEmpty() || hasNonMetricInput(agg, aliases)) {
            return agg;
        }
        Expression conditional = null;
        for (Attribute metric : metrics) {
            // Create an is not null check for each metric
            if (conditional == null) {
                conditional = new IsNotNull(agg.source(), metric);
            } else {
                // Join the is not null checks with OR nodes
                conditional = new Or(agg.source(), conditional, new IsNotNull(agg.source(), metric));
            }
        }
        return agg.replaceChild(new Filter(agg.source(), agg.child(), conditional));
    }

    /**
     * Whether an aggregation reads an input that isn't derived from metrics alone, such as a value computed with {@code COALESCE} or
     * one that reads a field that's neither a metric, a dimension, nor the timestamp. A filter on the metrics alone would drop the rows
     * that only have values for that input.
     * <p>
     * In that case, we skip the filter entirely rather than also filtering on the non-metric inputs. Nulls of non-metric fields have
     * never been filtered out, and aggregations rely on seeing them, like {@code absent_over_time} or buckets reporting {@code null}.
     * </p>
     */
    private static boolean hasNonMetricInput(TimeSeriesAggregate agg, AttributeMap<Expression> aliases) {
        Holder<Boolean> found = new Holder<>(false);
        agg.forEachExpression(AggregateFunction.class, af -> {
            // VALUES and DIMENSION_VALUES only carry the grouping keys from the per time series aggregation to the final one
            if (af instanceof Values || af instanceof DimensionValues) {
                return;
            }
            for (Expression field : af.fields()) {
                Set<Attribute> roots = new LinkedHashSet<>();
                if (resolveRoots(field, aliases, roots) == false) {
                    found.set(true);
                    return;
                }
                for (Attribute root : roots) {
                    if (root.isMetric() == false && root.isDimension() == false && root.semanticEquals(agg.timestamp()) == false) {
                        found.set(true);
                        return;
                    }
                }
            }
        });
        return found.get();
    }

    /**
     * Collects the attributes an expression reads, following aliases, into {@code roots}. Returns {@code false} if the expression
     * reads an attribute through an expression that isn't {@link AnyNullIsNull}: such an expression may be non-null when its inputs
     * are null, so it can't be traced back to the fields it reads. Constants don't contribute any roots.
     */
    private static boolean resolveRoots(Expression expression, AttributeMap<Expression> aliases, Set<Attribute> roots) {
        Expression resolved = aliases.resolve(expression, expression);
        if (resolved instanceof Attribute attribute) {
            roots.add(attribute);
            return true;
        }
        if (resolved.foldable()) {
            return true;
        }
        if (resolved instanceof AnyNullIsNull == false) {
            return false;
        }
        for (Expression child : resolved.children()) {
            if (resolveRoots(child, aliases, roots) == false) {
                return false;
            }
        }
        return true;
    }

    private static AttributeMap<Expression> aliases(LogicalPlan plan) {
        AttributeMap.Builder<Expression> aliases = AttributeMap.builder();
        plan.forEachExpressionDown(Alias.class, alias -> aliases.put(alias.toAttribute(), alias.child()));
        return aliases.build();
    }

    /**
     * Scans the given {@link LogicalPlan} to see if it is a "metrics mode" query
     */
    private static boolean isMetricsQuery(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof EsRelation r) {
            return r.indexMode().isTsdb();
        }
        if (logicalPlan instanceof UnresolvedRelation r) {
            return r.indexMode().isTsdb();
        }
        return false;
    }
}
