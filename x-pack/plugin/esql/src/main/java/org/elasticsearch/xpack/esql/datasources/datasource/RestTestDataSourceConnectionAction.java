/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST /_query/data_source/_test}.
 * Accepts the full data source configuration in the request body and opens a live connection to verify
 * the settings are reachable. The data source does not need to exist in cluster state — this endpoint
 * is intended for validating a new configuration before saving it.
 * Response: {@code {"status": "success"}}, {@code {"status": "failure", "error": "..."}}, or
 * {@code {"status": "untestable"[, "message": "..."]}}. Returns 400 for an unregistered type.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestTestDataSourceConnectionAction extends BaseRestHandler {

    // Mirrors HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG without importing
    // the http-datasource plugin (wrong dependency direction). Both check the same JVM property.
    private static final FeatureFlag LOCAL_TYPE_FLAG = new FeatureFlag("esql_external_datasources_local");

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/data_source/_test"));
    }

    @Override
    public String getName() {
        return "esql_test_data_source_connection";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final TestDataSourceConnectionAction.Request req = TestDataSourceConnectionAction.Request.fromXContent(request.contentParser());
        return channel -> client.execute(TestDataSourceConnectionAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        Set<String> caps = new HashSet<>(
            Set.of(
                EsqlDataSourcesCapabilities.DATA_SOURCES,
                EsqlDataSourcesCapabilities.DATA_SOURCES_SERVERLESS_SCOPE,
                EsqlDataSourcesCapabilities.DATA_SOURCE_TEST_CONNECTION
            )
        );
        if (LOCAL_TYPE_FLAG.isEnabled()) {
            caps.add(EsqlDataSourcesCapabilities.DATA_SOURCE_LOCAL_TYPE);
        }
        return Set.copyOf(caps);
    }
}
