/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds an {@link EsqlStatement} containing a {@link PromqlCommand} logical plan
 * directly from Prometheus query parameters, bypassing ES|QL string construction and parsing.
 */
class PromqlQueryPlanBuilder {

    private static final Duration DEFAULT_SCRAPE_INTERVAL = Duration.ofMinutes(1);

    record PromqlStatementResult(EsqlStatement esqlStatement, String resultType) {}

    /**
     * Builds an {@link EsqlStatement} containing a {@link PromqlCommand} with an {@link Eval} node
     * for the {@code TO_LONG(step)} conversion used by the Prometheus response writer.
     */
    static PromqlStatementResult buildStatement(
        String query,
        String index,
        String startStr,
        String endStr,
        String stepStr,
        QueryMode mode
    ) {
        return buildStatement(query, index, startStr, endStr, stepStr, 0, mode);
    }

    static PromqlStatementResult buildStatement(
        String query,
        String index,
        String startStr,
        String endStr,
        String stepStr,
        int limit,
        QueryMode mode
    ) {
        Instant startInstant = PromqlParserUtils.parseDate(Source.EMPTY, startStr);
        Instant endInstant = PromqlParserUtils.parseDate(Source.EMPTY, endStr);
        Duration stepDuration = parseStep(stepStr);
        Literal startLiteral = Literal.dateTime(Source.EMPTY, startInstant);
        Literal endLiteral = Literal.dateTime(Source.EMPTY, endInstant);
        Literal stepLiteral = Literal.timeDuration(Source.EMPTY, stepDuration);
        return buildStatement(query, index, startLiteral, endLiteral, stepLiteral, Literal.NULL, limit, mode);
    }

    static PromqlStatementResult buildStatement(
        String query,
        String index,
        Instant startInstant,
        Instant endInstant,
        Duration stepDuration,
        QueryMode mode
    ) {
        Literal startLiteral = Literal.dateTime(Source.EMPTY, startInstant);
        Literal endLiteral = Literal.dateTime(Source.EMPTY, endInstant);
        Literal stepLiteral = Literal.timeDuration(Source.EMPTY, stepDuration);
        return buildStatement(query, index, startLiteral, endLiteral, stepLiteral, Literal.NULL, 0, mode);
    }

    static PromqlStatementResult buildStatement(String query, String index, Instant at, QueryMode mode) {
        Literal timeLiteral = Literal.dateTime(Source.EMPTY, at);
        return buildStatement(query, index, timeLiteral, timeLiteral, Literal.NULL, Literal.NULL, 0, mode);
    }

    private static PromqlStatementResult buildStatement(
        String query,
        String index,
        Literal startLiteral,
        Literal endLiteral,
        Literal stepLiteral,
        Literal bucketsLiteral,
        int limit,
        QueryMode mode
    ) {
        IndexPattern indexPattern = new IndexPattern(Source.EMPTY, index);
        UnresolvedRelation unresolvedRelation = new UnresolvedRelation(
            Source.EMPTY,
            indexPattern,
            false,
            List.of(),
            null,
            SourceCommand.PROMQL
        );

        PromqlParser promqlParser = new PromqlParser();
        LogicalPlan promqlPlan = promqlParser.createStatement(query, startLiteral, endLiteral, 0, 0);

        PromqlCommand promqlCommand = new PromqlCommand(
            Source.EMPTY,
            unresolvedRelation,
            promqlPlan,
            startLiteral,
            endLiteral,
            stepLiteral,
            bucketsLiteral,
            Literal.timeDuration(Source.EMPTY, DEFAULT_SCRAPE_INTERVAL),
            PrometheusQueryResponseListener.VALUE_COLUMN,
            new UnresolvedTimestamp(Source.EMPTY)
        );

        // Wrap in TimeSeriesCollapse so PrometheusQueryResponseListener reads one MV row per series.
        // Bounds (start/end/stepMillis) are populated by the lowering rule from the PromqlCommand.
        TimeSeriesCollapse collapse = new TimeSeriesCollapse(
            Source.EMPTY,
            promqlCommand,
            promqlCommand.valueAttribute(),
            promqlCommand.stepAttribute(),
            new ArrayList<>(promqlPlan.output())
        );

        // TO_LONG converts the collapsed MV step datetime column to epoch millis so the response listener reads Long values directly.
        Alias stepAlias = new Alias(
            Source.EMPTY,
            promqlCommand.stepColumnName(),
            new ToLong(
                Source.EMPTY,
                collapse.output().stream().filter(a -> a.name().equals(promqlCommand.stepColumnName())).findFirst().get()
            )
        );
        // Eval's mergeOutputAttributes drops step(datetime) and appends step_alias(long) at the end,
        // producing [value, ...dimensions, step(long)] — the order the response listener expects.
        Eval eval = new Eval(Source.EMPTY, collapse, List.of(stepAlias));

        // No OrderBy: TimeSeriesCollapseOperator emits each series' MV samples in fixed step-ordinal order.
        LogicalPlan plan = eval;
        if (limit > 0) {
            int sentinelLimit = limit == Integer.MAX_VALUE ? limit : limit + 1;
            plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, sentinelLimit, DataType.INTEGER), plan);
        }
        String resultType = computeResultType(promqlPlan, mode);
        return new PromqlStatementResult(new EsqlStatement(plan, PrometheusPlanBuilderUtils.QUERY_SETTINGS), resultType);
    }

    private static String computeResultType(LogicalPlan plan, QueryMode mode) {
        PromqlDataType type = PromqlLogicalPlanBuilder.returnType(plan);
        return switch (type) {
            case INSTANT_VECTOR -> mode == QueryMode.RANGE ? "matrix" : "vector";
            case RANGE_VECTOR -> "matrix";
            case SCALAR -> mode == QueryMode.RANGE ? "matrix" : "scalar";
            case null -> throw new IllegalArgumentException("expected PromqlPlan, got [" + plan.getClass().getSimpleName() + "]");
        };
    }

    private static Duration parseStep(String value) {
        try {
            return Duration.ofSeconds(Integer.parseInt(value));
        } catch (NumberFormatException ignore) {
            return PromqlParserUtils.parseDuration(Source.EMPTY, value);
        }
    }
}
