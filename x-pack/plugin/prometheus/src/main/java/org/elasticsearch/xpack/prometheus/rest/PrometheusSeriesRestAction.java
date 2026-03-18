/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for the Prometheus {@code GET /api/v1/series} endpoint.
 * Returns the list of time series matching one or more label selectors.
 *
 * <h2>Index targeting</h2>
 * <p>The optional {@code {dataset}} and {@code {namespace}} path parameters are combined with the
 * fixed type {@code metrics} to construct the index pattern {@code metrics-{dataset}-{namespace}}.
 * Both default to {@code *}, so the no-param route reads from {@code metrics-*-*}.
 *
 * <p>Unlike {@link PrometheusRemoteWriteRestAction}, {@code .prometheus} is <em>not</em> appended
 * to the dataset — callers supply the full dataset component (e.g. {@code generic.prometheus}) and
 * can target any metrics data stream, not only prometheus ones (e.g. {@code generic.otel}).
 *
 * <h2>Supported methods</h2>
 * <p>Only GET is supported. POST with {@code application/x-www-form-urlencoded} bodies is rejected
 * at the HTTP layer as a CSRF safeguard before this handler is ever reached — see
 * {@code RestController#isContentTypeDisallowed}.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusSeriesRestAction extends BaseRestHandler {

    private static final String MATCH_PARAM = "match[]";
    private static final String START_PARAM = "start";
    private static final String END_PARAM = "end";
    private static final String LIMIT_PARAM = "limit";

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
        // PathTrie requires all routes sharing the same wildcard position to use the same name.
        // PrometheusRemoteWriteRestAction already registers /_prometheus/{dataset}/{namespace}/api/v1/write,
        // so we must reuse {dataset} and {namespace} here.
        return List.of(
            new Route(GET, "/_prometheus/api/v1/series"),
            new Route(GET, "/_prometheus/{dataset}/api/v1/series"),
            new Route(GET, "/_prometheus/{dataset}/{namespace}/api/v1/series")
        );
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
        Instant start = startParam != null ? PromqlParserUtils.parseDate(Source.EMPTY, startParam) : end.minus(24, HOURS);

        // Optional limit; default to 10 000 to avoid unbounded ESQL scans
        int limit = request.paramAsInt(LIMIT_PARAM, 10_000);

        // Construct the index pattern from dataset and namespace (type is always "metrics").
        // Unlike the write endpoint, ".prometheus" is NOT appended to the dataset — callers supply
        // the full dataset component (e.g. "generic.prometheus") and can target any metrics data
        // stream, not only prometheus ones. Both default to "*" so the no-param route reads from
        // "metrics-*-*".
        String dataset = request.param(DataStream.DATASET, "*");
        String namespace = request.param(DataStream.NAMESPACE, "*");
        String index = "metrics-" + dataset + "-" + namespace;

        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan(index, matchSelectors, start, end, limit);
        EsqlStatement statement = new EsqlStatement(plan, List.of());
        EsqlQueryRequest esqlRequest = EsqlQueryRequest.syncEsqlQueryRequestWithPlan(statement);

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
        List<String> matches = parseQueryString(uri.substring(queryIdx + 1)).get(MATCH_PARAM);
        return matches != null ? matches : List.of();
    }

    /**
     * Parses a URL-encoded query string into a multi-value map.
     */
    static Map<String, List<String>> parseQueryString(String queryString) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (queryString == null || queryString.isBlank()) {
            return result;
        }
        for (String pair : queryString.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String val = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
        }
        return result;
    }
}
