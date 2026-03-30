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
 * Limit(limit==0 ? MAX_VALUE : limit+1)
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
     * @param limit          maximum number of label names to return (0 = disabled, defers to ESQL max)
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

        // Sort ascending — the Prometheus HTTP API spec does not mandate ordering, but the reference
        // implementation sorts at the TSDB layer so clients conventionally expect alphabetical order.
        plan = new OrderBy(
            Source.EMPTY,
            plan,
            List.of(new Order(Source.EMPTY, dimField, Order.OrderDirection.ASC, Order.NullsPosition.LAST))
        );

        // limit=0 means "unlimited" in Prometheus semantics. We still emit an explicit LIMIT so
        // that ESQL's AddImplicitLimit rule sees an existing node and does not silently inject its
        // own default (1 000 rows + a warning header). Integer.MAX_VALUE is then quietly capped to
        // esql.query.result_truncation_max_size (default 10 000) without a warning.
        // limit>0 uses limit+1 as a sentinel: if the response contains exactly limit+1 rows we
        // know the result was truncated and the listener emits a Prometheus warnings field.
        int limitValue = limit > 0 ? limit + 1 : Integer.MAX_VALUE;
        plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, limitValue), plan);
        return plan;
    }
}
