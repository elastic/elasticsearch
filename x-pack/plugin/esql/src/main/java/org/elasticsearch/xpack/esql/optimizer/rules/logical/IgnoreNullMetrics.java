/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.HashSet;
import java.util.Set;

/**
 * TSDB often ends up storing many null values for metrics columns (since not every time series contains every metric).  However, loading
 * many null values can negatively impact query performance.  To reduce that, this rule applies filters to remove null values on all
 * metrics involved in the query.  In the case that there are multiple metrics, the not null checks are OR'd together, so we accept rows
 * where any of the metrics have values.
 */
public final class IgnoreNullMetrics extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        return logicalPlan.transformUp(TimeSeriesAggregate.class, agg -> {
            Set<Attribute> metrics = new HashSet<>();
            agg.forEachExpression(Attribute.class, attr -> {
                if (attr.isMetric()) {
                    metrics.add(attr);
                }
            });
            if (metrics.isEmpty()) {
                return agg;
            }
            Expression conditional = null;
            for (Attribute metric : metrics) {
                // Create an is not null check for each metric
                if (conditional == null) {
                    conditional = new IsNotNull(logicalPlan.source(), metric);
                } else {
                    // Join the is not null checks with OR nodes
                    conditional = new Or(logicalPlan.source(), conditional, new IsNotNull(Source.EMPTY, metric));
                }
            }
            Expression finalConditional = conditional;
            return agg.transformUp(p -> isMetricsQuery((LogicalPlan) p), p -> new Filter(p.source(), p, finalConditional));
        });
    }

    /**
     * Scans the given {@link LogicalPlan} to see if it is a "metrics mode" query
     */
    private static boolean isMetricsQuery(LogicalPlan logicalPlan) {
        if (logicalPlan instanceof EsRelation r) {
            return r.indexMode() == IndexMode.TIME_SERIES;
        }
        if (logicalPlan instanceof UnresolvedRelation r) {
            return r.indexMode() == IndexMode.TIME_SERIES;
        }
        return false;
    }
}
