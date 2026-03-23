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
import org.elasticsearch.rest.RestUtils;
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
import java.util.Set;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for the Prometheus {@code GET /api/v1/series} endpoint.
 * Returns the list of time series matching one or more label selectors.
 * Only GET is supported. POST with {@code application/x-www-form-urlencoded} bodies is rejected
 * at the HTTP layer as a CSRF safeguard before this handler is ever reached — see
 * {@code RestController#isContentTypeDisallowed}.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusSeriesRestAction extends BaseRestHandler {

    private static final String MATCH_PARAM = "match[]";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";
    private static final String LIMIT_PARAM = "limit";

    private static final int DEFAULT_LIMIT = 10_000;
    private static final long DEFAULT_LOOKBACK_HOURS = 24;

    @Override
    public String getName() {
        return "prometheus_series_action";
    }

    @Override
    protected Set<String> responseParams() {
        // match[] is parsed directly from the URI (to support multiple values) rather than via
        // request.param(), so it must be declared here to avoid "unrecognized parameter" errors.
        return Set.of(MATCH_PARAM);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/series"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        List<String> matchSelectors = collectMatchSelectors(request);
        if (matchSelectors.isEmpty()) {
            throw new IllegalArgumentException("At least one [match[]] selector is required");
        }

        // Time range
        String endParam = request.param(END_PARAM);
        String startParam = request.param(START_PARAM);
        Instant end = endParam != null ? PromqlParserUtils.parseDate(Source.EMPTY, endParam) : Instant.now();
        Instant start = startParam != null
            ? PromqlParserUtils.parseDate(Source.EMPTY, startParam)
            : end.minus(DEFAULT_LOOKBACK_HOURS, HOURS);

        // Optional limit; default to DEFAULT_LIMIT to avoid unbounded ESQL scans
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);

        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", matchSelectors, start, end, limit);
        EsqlStatement statement = new EsqlStatement(plan, List.of());
        PreparedEsqlQueryRequest esqlRequest = PreparedEsqlQueryRequest.sync(statement);

        return channel -> client.execute(EsqlQueryAction.INSTANCE, esqlRequest, new PrometheusSeriesResponseListener(channel));
    }

    /**
     * Collects {@code match[]} selectors from the URL query string.
     *
     * <p>The query string is parsed directly (not via {@code request.param()}) to correctly handle
     * multiple {@code match[]=...} entries — Prometheus clients send one entry per selector.
     */
    private static List<String> collectMatchSelectors(RestRequest request) {
        String uri = request.uri();
        int queryIdx = uri.indexOf('?');
        if (queryIdx < 0) {
            return List.of();
        }
        List<String> matches = RestUtils.decodeQueryStringMulti(uri, queryIdx + 1).get(MATCH_PARAM);
        return matches != null ? matches : List.of();
    }
}
