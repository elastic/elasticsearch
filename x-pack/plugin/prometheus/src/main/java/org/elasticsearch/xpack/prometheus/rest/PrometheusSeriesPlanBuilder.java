/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;

import java.time.Instant;
import java.util.List;

/**
 * Builds the {@link LogicalPlan} for a Prometheus {@code /api/v1/series} request.
 *
 * <p>The resulting plan has the shape:
 * <pre>
 * Limit(n)
 *   \_TsInfo
 *       \_Filter(timeCond AND OR(selectorConds...))
 *           \_UnresolvedRelation(index, TS)
 * </pre>
 */
final class PrometheusSeriesPlanBuilder {

    private PrometheusSeriesPlanBuilder() {}

    /**
     * Builds the logical plan for a series request.
     *
     * @param index          index pattern, e.g. {@code "*"} or a concrete name
     * @param matchSelectors list of {@code match[]} selector strings (at least one required)
     * @param start          start of the time range (inclusive)
     * @param end            end of the time range (inclusive)
     * @param limit          maximum number of series to return; {@code 0} means unlimited
     * @return the logical plan
     * @throws IllegalArgumentException if a selector is not a valid instant vector selector
     */
    static LogicalPlan buildPlan(String index, List<String> matchSelectors, Instant start, Instant end, int limit) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);
        plan = new Filter(Source.EMPTY, plan, PrometheusPlanBuilderUtils.filterExpression(matchSelectors, start, end));
        plan = new TsInfo(Source.EMPTY, plan);
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
