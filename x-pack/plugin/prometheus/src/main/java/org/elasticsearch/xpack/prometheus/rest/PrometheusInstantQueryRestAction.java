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
import org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;

/**
 * REST handler for the Prometheus {@code GET /api/v1/query} instant query endpoint.
 *
 * <p><b>Current implementation (temporary approximation):</b> translates the instant query into a
 * range query from {@code time - 5m} to {@code time} with a step of {@code 5m}, then returns only
 * the last sample per series. The fixed 5-minute window is a placeholder that makes the endpoint
 * functional without requiring ES|QL-level support for instant evaluation.
 *
 * <p><b>What needs to change for a proper implementation:</b>
 * <ul>
 *   <li>The PromQL engine should evaluate the expression at a single point in time rather than
 *       over a range. This requires a dedicated "instant" execution mode in {@link PromqlQueryPlanBuilder}
 *       and the underlying {@code PromqlCommand}, passing only the evaluation timestamp instead of
 *       a start/end/step triple.</li>
 *   <li>The lookback delta (how far back the engine searches for a stale sample) should be
 *       configurable via the {@code lookback_delta} request parameter instead of being hardcoded
 *       to 5 minutes.</li>
 *   <li>Once the engine evaluates at a point in time natively, {@link PrometheusQueryResponseListener}
 *       no longer needs the "last sample wins" logic for {@code QueryMode#INSTANT} — the engine
 *       will already return exactly one value per series.</li>
 * </ul>
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries">Prometheus Instant Queries API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusInstantQueryRestAction extends BaseRestHandler {

    private static final String INDEX_PARAM = "index";
    private static final String QUERY_PARAM = "query";
    private static final String TIME_PARAM = "time";
    private static final String LIMIT_PARAM = "limit";
    private static final int DEFAULT_LIMIT = 0; // 0 = no limit, matching Prometheus semantics
    private static final String STEP = "5m";

    @Override
    public String getName() {
        return "prometheus_instant_query_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/query"), new Route(GET, "/_prometheus/{index}/api/v1/query"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String index = request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);

        String timeStr = request.param(TIME_PARAM);
        Instant endInstant = timeStr != null && timeStr.isEmpty() == false
            ? PromqlParserUtils.parseDate(Source.EMPTY, timeStr)
            : Instant.now();

        Instant startInstant = endInstant.minusSeconds(5 * 60);
        String start = DateTimeFormatter.ISO_INSTANT.format(startInstant);
        String end = DateTimeFormatter.ISO_INSTANT.format(endInstant);

        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement(query, index, start, end, STEP);
        var esqlRequest = PreparedEsqlQueryRequest.sync(statement, query);

        return channel -> client.execute(
            EsqlQueryAction.INSTANCE,
            esqlRequest,
            new PrometheusQueryResponseListener(
                channel,
                PrometheusQueryResponseListener.QueryMode.INSTANT,
                limit == 0 ? Integer.MAX_VALUE : limit
            )
        );
    }

    private static String getRequiredParam(RestRequest request, String name) {
        String value = request.param(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("required parameter \"" + name + "\" is missing");
        }
        return value;
    }

}
