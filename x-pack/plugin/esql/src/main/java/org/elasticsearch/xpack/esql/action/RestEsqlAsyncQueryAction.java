/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlAsyncQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlAsyncQueryAction.class);

    private final EsqlCapabilities capabilities;
    private final CrossProjectModeDecider crossProjectModeDecider;

    public RestEsqlAsyncQueryAction(EsqlCapabilities capabilities, Settings settings) {
        this.capabilities = capabilities;
        this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
    }

    @Override
    public String getName() {
        return "esql_async_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/async"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities.capabilities();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            EsqlQueryRequest esqlRequest = RequestXContent.parseAsync(parser);
            esqlRequest.setResolvesCrossProject(crossProjectModeDecider.crossProjectEnabled() && esqlRequest.allowsCrossProject());
            return RestEsqlQueryAction.restChannelConsumer(esqlRequest, request, client);
        }
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, DROP_NULL_COLUMNS_OPTION);
    }
}
