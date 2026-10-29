/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
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
 */
public final class IgnoreNullMetrics extends OptimizerRules.OptimizerRule<TimeSeriesAggregate> {
    @Override
    public LogicalPlan rule(TimeSeriesAggregate agg) {
        AttributeMap<Attribute> castMetrics = metricsBehindCasts(agg.child());
        // Preserve expression order so equivalent plans produce the same null-filter predicate.
        Set<Attribute> metrics = new LinkedHashSet<>();
        agg.forEachExpression(Attribute.class, attr -> {
            Attribute metric = metricOf(attr, castMetrics);
            if (metric != null) {
                metrics.add(metric);
            }
        });
        if (metrics.isEmpty() || hasNonMetricInput(agg, castMetrics)) {
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
     * Whether an aggregation reads an input that isn't a metric, such as a value computed with arithmetic or {@code COALESCE}
     * in an {@code EVAL}. Casts and aliases of metrics retain metric identity. A filter on the metrics alone would drop the rows
     * that only have values for that input.
     * <p>
     * In that case, we skip the filter entirely rather than also filtering on the non-metric inputs. Nulls of non-metric fields have
     * never been filtered out, and aggregations rely on seeing them, like {@code absent_over_time} or buckets reporting {@code null}.
     * </p>
     */
    private static boolean hasNonMetricInput(TimeSeriesAggregate agg, AttributeMap<Attribute> castMetrics) {
        Holder<Boolean> found = new Holder<>(false);
        agg.forEachExpression(AggregateFunction.class, af -> {
            // VALUES and DIMENSION_VALUES only carry the grouping keys from the per time series aggregation to the final one
            if (af instanceof Values || af instanceof DimensionValues) {
                return;
            }
            for (Expression field : af.fields()) {
                for (Attribute attr : field.references()) {
                    if (metricOf(attr, castMetrics) == null
                        && attr.isDimension() == false
                        && attr.semanticEquals(agg.timestamp()) == false) {
                        found.set(true);
                    }
                }
            }
        });
        return found.get();
    }

    private static Attribute metricOf(Attribute attr, AttributeMap<Attribute> castMetrics) {
        // Implicit aggregate_metric_double conversions can filter the source field directly, allowing pushdown.
        Attribute sourceMetric = castMetrics.get(attr);
        return sourceMetric != null ? sourceMetric : attr.isMetric() ? attr : null;
    }

    private static AttributeMap<Attribute> metricsBehindCasts(LogicalPlan plan) {
        AttributeMap.Builder<Attribute> castMetrics = AttributeMap.builder();
        while (plan instanceof Eval eval) {
            for (Alias alias : eval.fields()) {
                if (alias.child() instanceof FromAggregateMetricDouble cast
                    && cast.field() instanceof Attribute field
                    && field.isMetric()) {
                    castMetrics.put(alias.toAttribute(), field);
                }
            }
            plan = eval.child();
        }
        return castMetrics.build();
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
