/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;

import java.time.Instant;
import java.util.List;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;

/**
 * Builds the {@link LogicalPlan} for a Prometheus {@code /api/v1/metadata} request.
 *
 * <p><b>Plan shape:</b>
 * <pre>{@code
 * Limit(esqlLimit)
 * \_ Aggregate(groupings=[metric_name, metric_type, unit])
 *    \_ Eval(metric_type = MV_MIN(metric_type), unit = MV_MIN(unit))
 *       \_ MetricsInfo
 *          \_ Filter(@timestamp >= start AND @timestamp <= end [AND metric IS NOT NULL])
 *             \_ UnresolvedRelation(index, TS)
 * }</pre>
 *
 * <p>The ES|QL Limit caps the total number of rows fetched. A finite limit is only possible when
 * both {@code limit} and {@code limit_per_metric} are set: {@code (limit + 1) * limitPerMetric}.
 * When {@code limit_per_metric} is 0 (unbounded) the total row count per metric is unknown, so
 * {@link Integer#MAX_VALUE} is used, which defers to the cluster's
 * {@code esql.query.result_truncation_max_size} setting.
 */
final class PrometheusMetadataPlanBuilder {

    static final String METRIC_NAME_FIELD = "metric_name";
    static final String METRIC_TYPE_FIELD = "metric_type";
    static final String UNIT_FIELD = "unit";

    /**
     * The Prometheus {@code /api/v1/metadata} API has no time-range parameters. We nevertheless
     * apply a lookback window because Elasticsearch has no global metadata store — metric metadata
     * is discovered by visiting individual time series. Without a time filter the query would scan
     * the entire index, which is prohibitively expensive in large deployments.
     */
    private static final long LOOKBACK_HOURS = 24;

    private PrometheusMetadataPlanBuilder() {}

    /**
     * Builds the logical plan using a 24-hour lookback from now.
     */
    static LogicalPlan buildPlan(String index, String metric, int limit, int limitPerMetric) {
        Instant end = Instant.now();
        Instant start = end.minus(LOOKBACK_HOURS, HOURS);
        return buildPlan(index, metric, limit, limitPerMetric, start, end);
    }

    /**
     * Builds the logical plan for the given time range. Package-private for testing.
     */
    static LogicalPlan buildPlan(String index, String metric, int limit, int limitPerMetric, Instant start, Instant end) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);
        plan = new Filter(Source.EMPTY, plan, buildFilterCondition(start, end, metric));
        plan = new MetricsInfo(Source.EMPTY, plan);

        // MetricsInfo may return multi-valued metric_type or unit fields when backing indices within a
        // data stream disagree on those values. MV_MIN collapses them to a single value before the
        // Aggregate so that grouping works correctly.
        UnresolvedAttribute metricTypeField = new UnresolvedAttribute(Source.EMPTY, METRIC_TYPE_FIELD);
        UnresolvedAttribute unitField = new UnresolvedAttribute(Source.EMPTY, UNIT_FIELD);
        plan = new Eval(
            Source.EMPTY,
            plan,
            List.of(
                new Alias(Source.EMPTY, METRIC_TYPE_FIELD, new MvMin(Source.EMPTY, metricTypeField)),
                new Alias(Source.EMPTY, UNIT_FIELD, new MvMin(Source.EMPTY, unitField))
            )
        );

        // Deduplicates rows across data streams that match on (metric_name, metric_type, unit) while
        // preserving distinct definitions when data streams diverge on type or unit.
        UnresolvedAttribute metricNameField = new UnresolvedAttribute(Source.EMPTY, METRIC_NAME_FIELD);
        metricTypeField = new UnresolvedAttribute(Source.EMPTY, METRIC_TYPE_FIELD);
        unitField = new UnresolvedAttribute(Source.EMPTY, UNIT_FIELD);
        List<Expression> groupings = List.of(metricNameField, metricTypeField, unitField);
        List<NamedExpression> aggregates = List.of(metricNameField, metricTypeField, unitField);
        plan = new Aggregate(Source.EMPTY, plan, groupings, aggregates);
        plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, computeEsqlLimit(limit, limitPerMetric)), plan);
        return plan;
    }

    private static Expression buildFilterCondition(Instant start, Instant end, String metric) {
        Expression timeCondition = PrometheusPlanBuilderUtils.buildTimeCondition(start, end);
        if (metric == null) {
            return timeCondition;
        }
        return combineAnd(List.of(timeCondition, new IsNotNull(Source.EMPTY, new UnresolvedAttribute(Source.EMPTY, metric))));
    }

    /**
     * Computes the ES|QL Limit value. A finite bound is only achievable when both Prometheus limits
     * are set: {@code (limit + 1) * limitPerMetric} — the +1 is the sentinel for truncation
     * detection. In all other cases {@link Integer#MAX_VALUE} is used so that ESQL silently caps
     * results at the cluster's {@code esql.query.result_truncation_max_size} setting.
     */
    private static int computeEsqlLimit(int limit, int limitPerMetric) {
        if (limit <= 0 || limitPerMetric <= 0) {
            return Integer.MAX_VALUE;
        }
        long esqlLimit = ((long) limit + 1) * limitPerMetric;
        return esqlLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) esqlLimit;
    }
}
