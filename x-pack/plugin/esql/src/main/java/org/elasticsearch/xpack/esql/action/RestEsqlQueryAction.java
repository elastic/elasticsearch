/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlQueryAction extends BaseRestHandler {
    private static final Logger LOGGER = LogManager.getLogger(RestEsqlQueryAction.class);

    @Override
    public String getName() {
        return "esql_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return EsqlCapabilities.CAPABILITIES;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            return restChannelConsumer(RequestXContent.parseSync(parser), request, client);
        }
    }

    protected static RestChannelConsumer restChannelConsumer(EsqlQueryRequest esqlRequest, RestRequest request, NodeClient client) {
        final Boolean partialResults = request.paramAsBoolean("allow_partial_results", null);
        if (partialResults != null) {
            esqlRequest.allowPartialResults(partialResults);
        }
        LOGGER.debug("Beginning execution of ESQL query.\nQuery string: [{}]", esqlRequest.query());

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(
                EsqlQueryAction.INSTANCE,
                esqlRequest,
                new EsqlResponseListener(channel, request, esqlRequest).wrapWithLogging()
            );
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION);
    }
}
