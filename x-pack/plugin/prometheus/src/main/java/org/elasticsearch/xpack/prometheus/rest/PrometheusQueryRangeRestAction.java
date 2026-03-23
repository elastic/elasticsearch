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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;

/**
 * REST handler for the Prometheus {@code GET /api/v1/query_range} endpoint.
 * Translates Prometheus query_range parameters into an ES|QL {@code PROMQL} command,
 * executes it, and converts the result into the Prometheus matrix JSON format.
 * Only GET is supported. POST with {@code application/x-www-form-urlencoded} bodies is rejected
 * at the HTTP layer as a CSRF safeguard before this handler is ever reached — see
 * {@code RestController#isContentTypeDisallowed}.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries">Prometheus Range Queries API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusQueryRangeRestAction extends BaseRestHandler {

    static final String QUERY_PARAM = "query";
    static final String START_PARAM = "start";
    static final String END_PARAM = "end";
    static final String INDEX_PARAM = "index";

    static final String ESQL_QUERY = "PROMQL step=?"
        + PrometheusQueryRangeResponseListener.STEP_PARAM
        + " start=?"
        + START_PARAM
        + " end=?"
        + END_PARAM
        + " index=?"
        + INDEX_PARAM
        + " "
        + PrometheusQueryRangeResponseListener.VALUE_COLUMN
        + "=(?"
        + QUERY_PARAM
        + ") | EVAL step = TO_LONG(step)";

    @Override
    public String getName() {
        return "prometheus_query_range_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/query_range"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String query = getRequiredParam(request, QUERY_PARAM);
        String start = getRequiredParam(request, START_PARAM);
        String end = getRequiredParam(request, END_PARAM);
        String step = getRequiredParam(request, PrometheusQueryRangeResponseListener.STEP_PARAM);

        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest(ESQL_QUERY);
        esqlRequest.params(buildQueryParams(query, "*", start, end, step));

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
     * Creates query parameters for all Prometheus query_range values.
     */
    static QueryParams buildQueryParams(String query, String index, String start, String end, String step) {
        return new QueryParams(
            List.of(
                new QueryParam(QUERY_PARAM, query, DataType.KEYWORD, VALUE),
                new QueryParam(INDEX_PARAM, index, DataType.KEYWORD, VALUE),
                new QueryParam(START_PARAM, start, DataType.KEYWORD, VALUE),
                new QueryParam(END_PARAM, end, DataType.KEYWORD, VALUE),
                new QueryParam(PrometheusQueryRangeResponseListener.STEP_PARAM, step, DataType.KEYWORD, VALUE)
            )
        );
    }

}
