/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Builds the {@link LogicalPlan} for a Prometheus {@code /api/v1/labels} request.
 *
 * <p>The resulting plan has the shape:
 * <pre>
 * [Limit(n)]
 *   └── OrderBy([dimension_fields ASC])
 *         └── Aggregate([dimension_fields], [dimension_fields])  -- STATS BY for dedup
 *               └── MvExpand(dimension_fields)
 *                     └── TsInfo
 *                           └── Filter(timeCond [AND OR(selectorConds...)])
 *                                 └── UnresolvedRelation(index, TS)
 * </pre>
 *
 * <p>The response listener strips the {@code "labels."}
 * prefix from each deduplicated label name and always prepends {@code __name__} to signal to
 * clients that the metric-name label is available for all metrics (both Prometheus and OTel).
 */
final class PrometheusLabelsPlanBuilder {

    private PrometheusLabelsPlanBuilder() {}

    /**
     * Builds the logical plan for a labels request.
     *
     * @param index          index pattern, e.g. {@code "*"} or a concrete name
     * @param matchSelectors list of {@code match[]} selector strings (may be empty)
     * @param start          start of the time range (inclusive)
     * @param end            end of the time range (inclusive)
     * @param limit          maximum number of label names to return (0 = disabled)
     * @return the logical plan
     * @throws IllegalArgumentException if a selector is not a valid instant vector selector
     */
    static LogicalPlan buildPlan(String index, List<String> matchSelectors, Instant start, Instant end, int limit) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);
        plan = new Filter(Source.EMPTY, plan, PrometheusPlanBuilderUtils.filterExpression(matchSelectors, start, end));
        plan = new TsInfo(Source.EMPTY, plan);

        // Expand the multivalued dimension_fields column into one row per label name
        UnresolvedAttribute dimField = new UnresolvedAttribute(Source.EMPTY, PrometheusPlanBuilderUtils.DIMENSION_FIELDS);
        ReferenceAttribute dimFieldExpanded = new ReferenceAttribute(
            Source.EMPTY,
            null,
            PrometheusPlanBuilderUtils.DIMENSION_FIELDS,
            KEYWORD
        );
        plan = new MvExpand(Source.EMPTY, plan, dimField, dimFieldExpanded);

        // Deduplicate via STATS BY dimension_fields
        plan = new Aggregate(Source.EMPTY, plan, List.of(dimField), List.of(dimField));

        // Sort ascending
        plan = new OrderBy(
            Source.EMPTY,
            plan,
            List.of(new Order(Source.EMPTY, dimField, Order.OrderDirection.ASC, Order.NullsPosition.LAST))
        );

        if (limit > 0) {
            plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, limit), plan);
        }
        return plan;
    }
}
