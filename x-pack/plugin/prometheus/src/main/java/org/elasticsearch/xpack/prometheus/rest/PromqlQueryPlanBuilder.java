/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Builds an {@link EsqlStatement} containing a {@link PromqlCommand} logical plan
 * directly from Prometheus query_range parameters, bypassing ES|QL string construction and parsing.
 */
class PromqlQueryPlanBuilder {

    private static final Duration DEFAULT_SCRAPE_INTERVAL = Duration.ofMinutes(1);

    /**
     * Builds an {@link EsqlStatement} containing a {@link PromqlCommand} with an {@link Eval} node
     * for the {@code TO_LONG(step)} conversion.
     */
    static EsqlStatement buildStatement(String query, String index, String startStr, String endStr, String stepStr) {
        Instant startInstant = PromqlParserUtils.parseDate(Source.EMPTY, startStr);
        Instant endInstant = PromqlParserUtils.parseDate(Source.EMPTY, endStr);
        Duration stepDuration = parseStep(Source.EMPTY, stepStr);

        Literal startLiteral = Literal.dateTime(Source.EMPTY, startInstant);
        Literal endLiteral = Literal.dateTime(Source.EMPTY, endInstant);
        Literal stepLiteral = Literal.timeDuration(Source.EMPTY, stepDuration);

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
            Literal.NULL,
            Literal.timeDuration(Source.EMPTY, DEFAULT_SCRAPE_INTERVAL),
            PrometheusQueryRangeResponseListener.VALUE_COLUMN,
            new UnresolvedTimestamp(Source.EMPTY)
        );

        // TO_LONG converts the step datetime to epoch millis, avoiding the need to parse a date string in the response listener.
        Alias stepAlias = new Alias(Source.EMPTY, promqlCommand.stepColumnName(), new ToLong(Source.EMPTY, promqlCommand.stepAttribute()));
        // Eval's mergeOutputAttributes drops step(datetime) and appends step_alias(long) at the end,
        // producing [value, ...dimensions, step(long)] — the order the response listener expects.
        Eval eval = new Eval(Source.EMPTY, promqlCommand, List.of(stepAlias));

        // Sort by step (timestamp) ascending so Prometheus clients receive values in chronological order.
        Attribute stepOutput = stepAlias.toAttribute();
        Order stepOrder = new Order(Source.EMPTY, stepOutput, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        OrderBy orderBy = new OrderBy(Source.EMPTY, eval, List.of(stepOrder));

        return new EsqlStatement(orderBy, List.of());
    }

    private static Duration parseStep(Source source, String value) {
        try {
            return Duration.ofSeconds(Integer.parseInt(value));
        } catch (NumberFormatException ignore) {
            return PromqlParserUtils.parseDuration(source, value);
        }
    }
}
