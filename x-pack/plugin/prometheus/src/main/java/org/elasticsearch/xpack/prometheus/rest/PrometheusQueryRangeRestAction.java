/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for the Prometheus {@code /api/v1/query_range} endpoint.
 * Translates Prometheus query_range parameters into an ES|QL {@link PromqlCommand} logical plan,
 * executes it, and converts the result into the Prometheus matrix JSON format.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries">Prometheus Range Queries API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusQueryRangeRestAction extends BaseRestHandler {

    static final String QUERY_PARAM = "query";
    static final String START_PARAM = "start";
    static final String END_PARAM = "end";
    public static final String INDEX_PARAM = "index";

    private static final Duration DEFAULT_SCRAPE_INTERVAL = Duration.ofMinutes(1);

    @Override
    public String getName() {
        return "prometheus_query_range_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/query_range"), new Route(POST, "/_prometheus/api/v1/query_range"));
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        // We do not parse or allow "application/x-www-form-urlencoded" here.
        // Elasticsearch's core RestController.isContentTypeDisallowed blocks it as a CSRF protection mechanism.
        // Therefore, Prometheus clients sending POST requests must use query parameters.
        return request.hasContent() == false || request.getXContentType() != null;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String start = getRequiredParam(request, START_PARAM);
        String end = getRequiredParam(request, END_PARAM);
        String step = getRequiredParam(request, PrometheusQueryRangeResponseListener.STEP_PARAM);
        String index = request.param(INDEX_PARAM, "*");

        EsqlStatement statement = buildStatement(query, index, start, end, step);
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequestWithPlan(statement);

        return channel -> client.execute(EsqlQueryAction.INSTANCE, esqlRequest, new PrometheusQueryRangeResponseListener(channel));
    }

    private static String getRequiredParam(RestRequest request, String name) {
        String value = request.param(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("required parameter \"" + name + "\" is missing");
        }
        return value;
    }

    /**
     * Builds an {@link EsqlStatement} containing a {@link PromqlCommand} with an {@link Eval} node
     * for the {@code TO_LONG(step)} conversion, bypassing ES|QL string construction and parsing.
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
        Alias stepAlias = new Alias(
            Source.EMPTY,
            PromqlCommand.STEP_COLUMN_NAME,
            new ToLong(
                Source.EMPTY,
                promqlCommand.output().stream().filter(a -> a.name().equals(PromqlCommand.STEP_COLUMN_NAME)).findFirst().get()
            )
        );
        Eval eval = new Eval(Source.EMPTY, promqlCommand, List.of(stepAlias));

        // Eval appends the replaced step column at the end; project to restore the original PromqlCommand column order.
        Project project = new Project(Source.EMPTY, eval, promqlCommand.output());

        return new EsqlStatement(project, List.of());
    }

    private static Duration parseStep(Source source, String value) {
        try {
            return Duration.ofSeconds(Integer.parseInt(value));
        } catch (NumberFormatException ignore) {
            return PromqlParserUtils.parseDuration(source, value);
        }
    }
}
