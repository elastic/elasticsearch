/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

public class RestEsqlQueryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "esql_query";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(Route.builder(POST, "/_esql").build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = EsqlQueryRequest.fromXContent(parser);
        }

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EsqlQueryAction.INSTANCE, esqlRequest, new EsqlResponseListener(channel, request, esqlRequest));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(URL_PARAM_DELIMITER);
    }
}
