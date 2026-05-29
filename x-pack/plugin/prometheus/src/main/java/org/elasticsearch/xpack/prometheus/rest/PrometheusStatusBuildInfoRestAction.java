/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.Build;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand.DEFAULT_PROMQL_INDEX_PATTERN;

/**
 * REST handler for {@code GET /_prometheus/api/v1/status/buildinfo}.
 * Returns build information in the standard Prometheus buildinfo format.
 * Grafana queries this endpoint to identify the backend type and determine available alerting features.
 * Without it, Grafana shows an error when opening the "Plugin info" panel in Metrics Drilldown.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusStatusBuildInfoRestAction extends BaseRestHandler {

    private static final String INDEX_PARAM = "index";

    @Override
    public String getName() {
        return "prometheus_status_buildinfo_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_prometheus/api/v1/status/buildinfo"),
            new Route(GET, "/_prometheus/{index}/api/v1/status/buildinfo")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Consume optional {index} path segment; response does not depend on index scope.
        request.param(INDEX_PARAM, DEFAULT_PROMQL_INDEX_PATTERN);
        Build build = Build.current();
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.field("status", "success");
            builder.startObject("data");
            {
                builder.field("application", "Elasticsearch");
                builder.field("version", build.version());
                builder.field("revision", build.hash());
                builder.field("buildDate", build.date());
            }
            builder.endObject();
        }
        builder.endObject();
        return channel -> channel.sendResponse(new RestResponse(RestStatus.OK, builder));
    }
}
