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
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatementResult;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;

/**
 * REST handler for the Prometheus {@code GET} and {@code POST /api/v1/query} instant query endpoint.
 *
 * Builds instant-query plan for the requested evaluation time.
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

    @Override
    public String getName() {
        return "prometheus_instant_query_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_prometheus/api/v1/query"),
            new Route(POST, "/_prometheus/api/v1/query"),
            new Route(GET, "/_prometheus/{index}/api/v1/query"),
            new Route(POST, "/_prometheus/{index}/api/v1/query")
        );
    }

    @Override
    public boolean supportsReadOnlyFormEncodedPostBody() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String index = request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);

        String timeStr = request.param(TIME_PARAM);
        Instant evaluationTime = timeStr != null && timeStr.isEmpty() == false
            ? PromqlParserUtils.parseDate(Source.EMPTY, timeStr)
            : Instant.now();

        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            query,
            index,
            evaluationTime,
            PrometheusQueryResponseListener.QueryMode.INSTANT
        );
        var esqlRequest = PreparedEsqlQueryRequest.sync(result.esqlStatement(), query);

        return channel -> client.execute(
            EsqlQueryAction.INSTANCE,
            esqlRequest,
            new PrometheusQueryResponseListener(
                channel,
                result.resultType(),
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
