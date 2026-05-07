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
import org.elasticsearch.xpack.esql.plan.EsqlStatement;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;

/**
 * REST handler for the Prometheus {@code GET /api/v1/query_range} endpoint.
 * Translates Prometheus query_range parameters into an ES|QL PromqlCommand logical plan,
 * executes it, and converts the result into the Prometheus matrix JSON format.
 * Only GET is supported. POST with {@code application/x-www-form-urlencoded} bodies is rejected
 * at the HTTP layer as a CSRF safeguard before this handler is ever reached — see
 * {@code RestController#isContentTypeDisallowed}.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries">Prometheus Range Queries API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusQueryRangeRestAction extends BaseRestHandler {

    private static final String INDEX_PARAM = "index";
    private static final String QUERY_PARAM = "query";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";
    private static final String LIMIT_PARAM = "limit";
    private static final int DEFAULT_LIMIT = 0; // 0 = no limit, matching Prometheus semantics

    @Override
    public String getName() {
        return "prometheus_query_range_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/query_range"), new Route(GET, "/_prometheus/{index}/api/v1/query_range"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String start = getRequiredParam(request, START_PARAM);
        String end = getRequiredParam(request, END_PARAM);
        String step = getRequiredParam(request, PrometheusQueryResponseListener.STEP_PARAM);
        String index = request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);

        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement(query, index, start, end, step);
        var esqlRequest = PreparedEsqlQueryRequest.sync(statement, query);

        return channel -> client.execute(
            EsqlQueryAction.INSTANCE,
            esqlRequest,
            new PrometheusQueryResponseListener(
                channel,
                PrometheusQueryResponseListener.QueryMode.RANGE,
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
