/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST /_query/data_source/_test_connection}.
 * Accepts the full data source configuration in the request body and opens a live connection to verify
 * the settings are reachable. The data source does not need to exist in cluster state — this endpoint
 * is intended for validating a new configuration before saving it.
 * Returns {@code {"connected": true}} on success or {@code {"connected": false, "error": "..."}} on failure;
 * returns 400 for an unregistered data source type.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestTestDataSourceConnectionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/data_source/_test_connection"));
    }

    @Override
    public String getName() {
        return "esql_test_data_source_connection";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final TestDataSourceConnectionAction.Request req = TestDataSourceConnectionAction.Request.fromXContent(request.contentParser());
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            TestDataSourceConnectionAction.INSTANCE,
            req,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(
            EsqlDataSourcesCapabilities.DATA_SOURCES,
            EsqlDataSourcesCapabilities.DATA_SOURCES_SERVERLESS_SCOPE,
            EsqlDataSourcesCapabilities.DATA_SOURCE_TEST_CONNECTION
        );
    }
}
