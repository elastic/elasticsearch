/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
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
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;
import static org.elasticsearch.xpack.prometheus.rest.PrometheusQueryRangeRestAction.getRequiredParam;

/**
 * REST handler for the Prometheus {@code GET} and {@code POST /api/v1/query_exemplars} endpoint, which returns the exemplars of the
 * series a PromQL query selects within a time range. The exemplars are read from the {@code exemplars-*} data streams that the OTLP
 * and Prometheus remote write endpoints write alongside the {@code metrics-*} ones.
 * <p>
 * The query is built like a query of the range query endpoint, with a step that makes its selectors read all samples of the time
 * range, or of the instant query endpoint if {@code start} equals {@code end}, and run with
 * {@link QuerySettings#EXEMPLARS SET exemplars} so that it returns the exemplars of the metrics its selectors select instead of its
 * result. Prometheus only uses the selectors of the query to select exemplars, not the expression around them. The
 * exemplars setting derives the same from the analyzed query (the metrics and label matchers of its selectors, and its evaluated time
 * range), so the query is run as is, and queries that the query endpoints reject are rejected here as well. Unlike Prometheus, the
 * exemplars of a selector with an {@code offset} are those of the samples it reads, which lie outside the requested time range when
 * the offset exceeds it, so that only exemplars of metric values the query actually uses are returned.
 * <p>
 * Unlike Prometheus, which defaults to an unbounded time range, {@code start} and {@code end} are required, like for the range query
 * endpoint.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars">Prometheus Querying Exemplars API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusQueryExemplarsRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(PrometheusQueryExemplarsRestAction.class);

    private static final String INDEX_PARAM = "index";
    private static final String QUERY_PARAM = "query";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";

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

    private final Supplier<TimeValue> maxQueryTimeout;

    /**
     * @param maxQueryTimeout supplies the default and maximum query timeout, see {@link PromqlQueryExecutor#resolveTimeout}
     */
    public PrometheusQueryExemplarsRestAction(Supplier<TimeValue> maxQueryTimeout) {
        this.maxQueryTimeout = maxQueryTimeout;
    }

    @Override
    public String getName() {
        return "prometheus_query_exemplars_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_prometheus/api/v1/query_exemplars"),
            new Route(POST, "/_prometheus/api/v1/query_exemplars"),
            new Route(GET, "/_prometheus/{index}/api/v1/query_exemplars"),
            new Route(POST, "/_prometheus/{index}/api/v1/query_exemplars")
        );
    }

    @Override
    public boolean supportsReadOnlyFormEncodedPostBody() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        try {
            return prepareQuery(request, client);
        } catch (Exception e) {
            // report invalid parameters in the Prometheus error format rather than the standard Elasticsearch one, and mark the
            // parameters that weren't read before the failure as consumed so that they aren't reported as unrecognized instead
            request.params().keySet().forEach(request::param);
            return channel -> PrometheusErrorResponse.send(channel, e, logger);
        }
    }

    private RestChannelConsumer prepareQuery(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String startParam = getRequiredParam(request, START_PARAM);
        String endParam = getRequiredParam(request, END_PARAM);
        String index = request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        TimeValue timeout = PromqlQueryExecutor.resolveTimeout(request, maxQueryTimeout.get());

        Instant start = PromqlParserUtils.parseDate(Source.EMPTY, startParam);
        Instant end = PromqlParserUtils.parseDate(Source.EMPTY, endParam);
        if (end.isBefore(start)) {
            throw new IllegalArgumentException("end timestamp must not be before start timestamp");
        }

        // A range query needs a positive step, so without a range it's an instant query instead.
        PromqlStatementResult result = start.equals(end)
            ? PromqlQueryPlanBuilder.buildStatement(query, index, start, QueryMode.INSTANT)
            : PromqlQueryPlanBuilder.buildStatement(query, index, start, end, step(query, start, end), QueryMode.RANGE);
        if (result.esqlStatement() == null) {
            // a string literal, which selects no series
            return channel -> channel.sendResponse(
                new RestResponse(RestStatus.OK, PrometheusQueryExemplarsResponseListener.emptyResponse())
            );
        }
        EsqlStatement statement = new EsqlStatement(result.esqlStatement().plan(), QUERY_SETTINGS);
        var esqlRequest = new PromqlQueryRequest(index, statement, query, START_PARAM, startParam, END_PARAM, endParam);
        return channel -> PromqlQueryExecutor.execute(
            client,
            request.getHttpChannel(),
            esqlRequest,
            timeout,
            new PrometheusQueryExemplarsResponseListener(channel, start, end)
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
            0,
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
