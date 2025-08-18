/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.HashSet;
import java.util.List;
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
        if (isMetricsQuery(logicalPlan) == false) {
            return logicalPlan;
        }
        Set<Attribute> metrics = collectMetrics(logicalPlan);
        if (metrics.isEmpty()) {
            return logicalPlan;
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
        // Find where to put the new Filter node; We want it right after the relation leaf node
        // We want to add the filter right after the TS command, which is a "leaf" node.
        // So we want to find the first node above the leaf node.
        return logicalPlan.transformUp(p -> isMetricsQuery((LogicalPlan) p), p -> new Filter(p.source(), p, finalConditional));
    }

    private Set<Attribute> collectMetrics(LogicalPlan logicalPlan) {
        Set<Attribute> metrics = new HashSet<>();
        logicalPlan.forEachDown(p -> {
            if (p instanceof UnaryPlan up) {
                for (Attribute attr : up.inputSet()) {
                    if (attr.isMetric()) {
                        metrics.add(attr);
                    }
                }
            }
        });
        return metrics;
    }

    /**
     * Scans the given {@link LogicalPlan} to see if it is a "metrics mode" query
     */
    private static boolean isMetricsQuery(LogicalPlan logicalPlan) {
        List<LogicalPlan> tsNodes = logicalPlan.collect(n -> {
            if (n instanceof EsRelation r) {
                return r.indexMode() == IndexMode.TIME_SERIES;
            }
            if (n instanceof UnresolvedRelation r) {
                return r.indexMode() == IndexMode.TIME_SERIES;
            }
            return false;
        });
        return tsNodes.isEmpty() == false;
    }
}
