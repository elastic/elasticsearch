/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.HashSet;
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
        // Only evals directly below the aggregate: every field they read passes through them, so the filter can reference it.
        AttributeMap.Builder<Expression> evaluated = AttributeMap.builder();
        for (LogicalPlan child = agg.child(); child instanceof Eval eval; child = eval.child()) {
            eval.fields().forEach(alias -> evaluated.put(alias.toAttribute(), alias.child()));
        }
        AttributeMap<Expression> definitions = evaluated.build();
        Set<Attribute> metrics = new HashSet<>();
        agg.forEachExpression(Attribute.class, attr -> {
            Attribute metric = metricOf(attr, definitions);
            if (metric != null) {
                metrics.add(metric);
            }
        });
        if (metrics.isEmpty()) {
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
     * The metric {@code expression} reads, directly or through a conversion between counter and gauge, or {@code null} if it
     * reads none. A conversion between counter and gauge keeps every value, so a row without the metric has no value of the
     * expression either, and filtering on the metric drops nothing the aggregate reads.
     */
    private static Attribute metricOf(Expression expression, AttributeMap<Expression> definitions) {
        return switch (expression) {
            case Attribute attr when attr.isMetric() -> attr;
            case Attribute attr -> {
                Expression definition = definitions.get(attr);
                yield definition == null ? null : metricOf(definition, definitions);
            }
            case ToGauge toGauge -> metricOf(toGauge.field(), definitions);
            case ToCounter toCounter -> metricOf(toCounter.field(), definitions);
            default -> null;
        };
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
