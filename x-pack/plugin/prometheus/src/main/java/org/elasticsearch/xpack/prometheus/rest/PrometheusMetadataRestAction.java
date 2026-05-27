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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;

/**
 * REST handler for the Prometheus {@code GET /_prometheus/api/v1/metadata} and
 * {@code GET /_prometheus/{index}/api/v1/metadata} endpoints.
 * Returns metric metadata (type, help, unit) for scraped metrics.
 * Uses the {@code METRICS_INFO} ES|QL command as data source.
 * When the path omits {@code {index}} and no {@code index} query parameter is set, the index
 * expression defaults to {@link PromqlCommand#DEFAULT_PROMQL_INDEX_PATTERN} (same as PromQL query APIs).
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusMetadataRestAction extends BaseRestHandler {

    private static final String LIMIT_PARAM = "limit";
    private static final String LIMIT_PER_METRIC_PARAM = "limit_per_metric";
    private static final String METRIC_PARAM = "metric";
    private static final String INDEX_PARAM = "index";

    private static final int DEFAULT_LIMIT = 0; // 0 = no explicit limit, matching Prometheus semantics

    @Override
    public String getName() {
        return "prometheus_metadata_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_prometheus/api/v1/metadata"), new Route(GET, "/_prometheus/{index}/api/v1/metadata"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        String metric = request.param(METRIC_PARAM);
        int limit = request.paramAsInt(LIMIT_PARAM, DEFAULT_LIMIT);
        int limitPerMetric = request.paramAsInt(LIMIT_PER_METRIC_PARAM, DEFAULT_LIMIT);

        LogicalPlan plan = PrometheusMetadataPlanBuilder.buildPlan(index, metric, limit, limitPerMetric);
        EsqlStatement statement = new EsqlStatement(plan, List.of());
        PreparedEsqlQueryRequest esqlRequest = PreparedEsqlQueryRequest.sync(statement, "prometheus_metadata");

        return channel -> client.execute(
            EsqlQueryAction.INSTANCE,
            esqlRequest,
            PrometheusMetadataResponseListener.create(channel, limit, limitPerMetric)
        );
    }
}
