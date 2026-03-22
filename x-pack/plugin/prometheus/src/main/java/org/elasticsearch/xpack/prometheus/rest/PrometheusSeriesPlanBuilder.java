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
 * [Limit(n)]
 *   └── TsInfo
 *         └── Filter(timeCond AND OR(selectorConds...))
 *               └── UnresolvedRelation(index, TS)
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
     * @param limit          maximum number of series to return (must be positive)
     * @return the logical plan
     * @throws IllegalArgumentException if a selector is not a valid instant vector selector
     */
    static LogicalPlan buildPlan(String index, List<String> matchSelectors, Instant start, Instant end, int limit) {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(index);
        plan = new Filter(Source.EMPTY, plan, PrometheusPlanBuilderUtils.filterExpression(matchSelectors, start, end));
        plan = new TsInfo(Source.EMPTY, plan);
        if (limit > 0) {
            plan = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, limit), plan);
        }
        return plan;
    }
}
