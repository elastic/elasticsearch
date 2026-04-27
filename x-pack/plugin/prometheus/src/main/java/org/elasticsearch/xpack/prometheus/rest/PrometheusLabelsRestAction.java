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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for the Prometheus {@code GET /api/v1/labels} endpoint.
 * Returns the sorted list of label names across all matching time series.
 * The optional {@code {index}} path parameter restricts the query to a specific index pattern;
 * when omitted, all indices ({@code "*"}) are searched.
 * Only GET is supported. POST with {@code application/x-www-form-urlencoded} bodies is rejected
 * at the HTTP layer as a CSRF safeguard before this handler is ever reached — see
 * {@code RestController#isContentTypeDisallowed}.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusLabelsRestAction extends BaseRestHandler {

    private static final String MATCH_PARAM = "match[]";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";
    private static final String LIMIT_PARAM = "limit";
    private static final String INDEX_PARAM = "index";

    private static final int DEFAULT_LIMIT = 0; // 0 = no limit, matching Prometheus semantics
    private static final long DEFAULT_LOOKBACK_HOURS = 24;

    @Override
    public String getName() {
        return "prometheus_labels_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/labels"), new Route(GET, "/_prometheus/{index}/api/v1/labels"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        List<String> matchSelectors = request.repeatedParamAsList(MATCH_PARAM);

        // Time range
        String endParam = request.param(END_PARAM);
        String startParam = request.param(START_PARAM);
        Instant end = endParam != null ? PromqlParserUtils.parseDate(Source.EMPTY, endParam) : Instant.now();
        Instant start = startParam != null
            ? PromqlParserUtils.parseDate(Source.EMPTY, startParam)
            : end.minus(DEFAULT_LOOKBACK_HOURS, HOURS);

        // Optional limit; 0 means "disabled" (Prometheus semantics), which defers to the ESQL
        // result_truncation_max_size cluster setting (default 10 000). Positive values use a
        // limit+1 sentinel to detect and report truncation.
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);

        String index = request.param(INDEX_PARAM, "*");
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan(index, matchSelectors, start, end, limit);
        EsqlStatement statement = new EsqlStatement(plan, List.of());
        PreparedEsqlQueryRequest esqlRequest = PreparedEsqlQueryRequest.sync(statement, "prometheus_labels");

        return channel -> client.execute(EsqlQueryAction.INSTANCE, esqlRequest, PrometheusLabelsResponseListener.create(channel, limit));
    }

}
