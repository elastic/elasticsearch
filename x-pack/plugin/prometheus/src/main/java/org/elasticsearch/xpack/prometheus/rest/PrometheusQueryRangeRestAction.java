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
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;

/**
 * REST handler for the Prometheus {@code /api/v1/query_range} endpoint.
 * Translates Prometheus query_range parameters into an ES|QL {@code PROMQL} command,
 * executes it, and converts the result into the Prometheus matrix JSON format.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries">Prometheus Range Queries API</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusQueryRangeRestAction extends BaseRestHandler {

    static final String QUERY_PARAM = "query";
    static final String START_PARAM = "start";
    static final String END_PARAM = "end";
    static final String STEP_PARAM = "step";
    static final String VALUE_COLUMN = "value";
    public static final String INDEX_PARAM = "index";

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
        String step = getRequiredParam(request, STEP_PARAM);
        String index = request.param(INDEX_PARAM, "*");

        String esqlQuery = buildEsqlQuery(query, index);
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequest(esqlQuery);
        esqlRequest.params(buildQueryParams(start, end, step));

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
     * Builds an ES|QL query string using named parameter placeholders for the start, end, and step values.
     * The {@code index} parameter controls which indices are queried (defaults to {@code *}).
     */
    static String buildEsqlQuery(String promqlQuery, String index) {
        return String.format(
            Locale.ROOT,
            "PROMQL step=?%s start=?%s end=?%s index=%s %s=(%s)",
            STEP_PARAM,
            START_PARAM,
            END_PARAM,
            index,
            VALUE_COLUMN,
            promqlQuery
        );
    }

    /**
     * Creates query parameters for the start, end, and step values.
     */
    static QueryParams buildQueryParams(String start, String end, String step) {
        return new QueryParams(
            List.of(
                new QueryParam(START_PARAM, start, DataType.KEYWORD, VALUE),
                new QueryParam(END_PARAM, end, DataType.KEYWORD, VALUE),
                new QueryParam(STEP_PARAM, step, DataType.KEYWORD, VALUE)
            )
        );
    }

}
