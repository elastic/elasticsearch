/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineOr;

/**
 * Builds the {@link LogicalPlan} for a Prometheus {@code /api/v1/label/{name}/values} request.
 *
 * <p>Two distinct plan shapes are used:
 *
 * <p><b>For {@code __name__}:</b>
 * <pre>
 * [Limit(limit+1)]
 *   └── OrderBy([metric_name ASC NULLS LAST])
 *         └── Aggregate(groupings=[metric_name])
 *               └── MetricsInfo
 *                     └── Filter(timeCond [AND OR(selectorConds...)])
 *                           └── UnresolvedRelation("*", TS)
 * </pre>
 *
 * <p><b>For regular labels (e.g. {@code job}):</b>
 * <pre>
 * [Limit(limit+1)]
 *   └── OrderBy([job ASC NULLS LAST])
 *         └── Aggregate(groupings=[job])
 *               └── Filter(timeCond AND IS_NOT_NULL(job) [AND OR(selectorConds...)])
 *                     └── UnresolvedRelation("*", TS)
 * </pre>
 *
 * <p>The Limit node uses {@code limit + 1} as a sentinel: if the result contains {@code limit + 1}
 * rows the response listener will truncate to {@code limit} and emit a warning. When {@code limit == 0}
 * the Limit node is omitted entirely.
 */
final class PrometheusLabelValuesPlanBuilder {

    /** The {@code __name__} pseudo-label backed by {@code metric_name} in the MetricsInfo output. */
    private static final String NAME_LABEL = "__name__";

    /** The field name produced by {@link MetricsInfo} for the metric name. */
    static final String METRIC_NAME_FIELD = "metric_name";

    private PrometheusLabelValuesPlanBuilder() {}

    /**
     * Builds the logical plan for a label values request.
     *
     * @param labelName      the decoded label name (e.g. {@code "job"} or {@code "__name__"})
     * @param index          index pattern, e.g. {@code "*"} or a concrete name
     * @param matchSelectors list of {@code match[]} selector strings (may be empty)
     * @param start          start of the time range (inclusive)
     * @param end            end of the time range (inclusive)
     * @param limit          maximum number of values to return (0 = disabled)
     * @return the logical plan
     * @throws IllegalArgumentException if a selector is not a valid instant vector selector
     */
    static LogicalPlan buildPlan(String labelName, String index, List<String> matchSelectors, Instant start, Instant end, int limit) {
        if (NAME_LABEL.equals(labelName)) {
            return buildNamePlan(index, matchSelectors, start, end, limit);
        } else {
            return buildRegularLabelPlan(labelName, index, matchSelectors, start, end, limit);
        }
    }

    private static LogicalPlan buildNamePlan(String index, List<String> matchSelectors, Instant start, Instant end, int limit) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);
        plan = new Filter(Source.EMPTY, plan, PrometheusPlanBuilderUtils.filterExpression(matchSelectors, start, end));
        plan = new MetricsInfo(Source.EMPTY, plan);

        UnresolvedAttribute metricNameField = new UnresolvedAttribute(Source.EMPTY, METRIC_NAME_FIELD);
        plan = new Aggregate(Source.EMPTY, plan, List.of(metricNameField), List.of(metricNameField));
        plan = new OrderBy(
            Source.EMPTY,
            plan,
            List.of(new Order(Source.EMPTY, metricNameField, Order.OrderDirection.ASC, Order.NullsPosition.LAST))
        );
        if (limit > 0) {
            plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, limit + 1), plan);
        }
        return plan;
    }

    private static LogicalPlan buildRegularLabelPlan(
        String labelName,
        String index,
        List<String> matchSelectors,
        Instant start,
        Instant end,
        int limit
    ) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);

        // Build filter: timeCond AND IS_NOT_NULL(labelName) [AND OR(selectorConds...)]
        UnresolvedAttribute labelField = new UnresolvedAttribute(Source.EMPTY, labelName);
        Expression isNotNull = new IsNotNull(Source.EMPTY, labelField);

        Expression timeCond = PrometheusPlanBuilderUtils.buildTimeCondition(start, end);
        List<Expression> selectorConditions = PrometheusPlanBuilderUtils.parseSelectorConditions(matchSelectors);

        List<Expression> filterParts = new ArrayList<>();
        filterParts.add(timeCond);
        filterParts.add(isNotNull);
        if (selectorConditions.isEmpty() == false) {
            filterParts.add(combineOr(selectorConditions));
        }
        Expression filterExpr = filterParts.size() == 1 ? filterParts.get(0) : combineAnd(filterParts);
        plan = new Filter(Source.EMPTY, plan, filterExpr);

        plan = new Aggregate(Source.EMPTY, plan, List.of(labelField), List.of(labelField));
        plan = new OrderBy(
            Source.EMPTY,
            plan,
            List.of(new Order(Source.EMPTY, labelField, Order.OrderDirection.ASC, Order.NullsPosition.LAST))
        );
        if (limit > 0) {
            plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, limit + 1), plan);
        }
        return plan;
    }
}
