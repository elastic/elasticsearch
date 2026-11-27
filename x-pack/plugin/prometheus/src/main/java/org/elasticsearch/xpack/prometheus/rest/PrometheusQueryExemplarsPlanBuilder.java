/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatementResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

/**
 * Builds the statement of the {@code query_exemplars} endpoint, see {@link PrometheusQueryExemplarsRestAction}: the PromQL query run
 * with {@link QuerySettings#EXEMPLARS SET exemplars}, so that it returns the exemplars of the metrics its selectors select instead
 * of its result.
 */
final class PrometheusQueryExemplarsPlanBuilder {

    /** Prometheus has no limit, but keeps a bounded number of exemplars in memory. This returns at most the most recent ones. */
    private static final int LIMIT = 10_000;

    /**
     * The settings of {@link PrometheusPlanBuilderUtils#QUERY_SETTINGS} plus
     * {@code SET exemplars={"include_timeseries": true, "limit": 10000}}: the {@code _timeseries} column identifies the series each
     * exemplar belongs to.
     */
    private static final List<QuerySetting> QUERY_SETTINGS = Stream.concat(
        PrometheusPlanBuilderUtils.QUERY_SETTINGS.stream(),
        Stream.of(
            new QuerySetting(
                Source.EMPTY,
                new Alias(
                    Source.EMPTY,
                    QuerySettings.EXEMPLARS.name(),
                    new MapExpression(
                        Source.EMPTY,
                        List.of(
                            Literal.keyword(Source.EMPTY, "include_timeseries"),
                            Literal.TRUE,
                            Literal.keyword(Source.EMPTY, "limit"),
                            new Literal(Source.EMPTY, LIMIT, DataType.INTEGER)
                        )
                    )
                )
            )
        )
    ).toList();

    private PrometheusQueryExemplarsPlanBuilder() {}

    /**
     * The query's statement with the settings of an exemplars query. A range query needs a positive step, so without a range it's
     * an instant query instead.
     */
    static PromqlStatementResult buildStatement(String query, String index, Instant start, Instant end) {
        PromqlStatementResult result = start.equals(end)
            ? PromqlQueryPlanBuilder.buildStatement(query, index, start, QueryMode.INSTANT)
            : PromqlQueryPlanBuilder.buildStatement(query, index, start, end, step(query, start, end), QueryMode.RANGE);
        return new PromqlStatementResult(
            new EsqlStatement(result.esqlStatement().plan(), QUERY_SETTINGS),
            result.resultType(),
            result.stringValue()
        );
    }

    /**
     * The step to evaluate the query with so that its selectors read all samples of {@code [start, end]}: a range selector whose range
     * is shorter than the step only reads the samples of the last range of each step, so the step is the shortest range of the
     * query's range selectors, or the whole time range if it's shorter or there are none. Instant selectors read at least one step
     * back, so they don't limit the step.
     */
    private static Duration step(String query, Instant start, Instant end) {
        Duration step = Duration.between(start, end);
        LogicalPlan plan = new PromqlParser().createStatement(
            query,
            Literal.dateTime(Source.EMPTY, start),
            Literal.dateTime(Source.EMPTY, end),
            1,
            0
        );
        for (RangeSelector selector : plan.collect(RangeSelector.class)) {
            if (selector.range().fold(FoldContext.small()) instanceof Duration range && range.isPositive() && range.compareTo(step) < 0) {
                step = range;
            }
        }
        return step;
    }
}
