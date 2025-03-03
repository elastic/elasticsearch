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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlGetAsyncResultAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/async/{id}"));
    }

    @Override
    public String getName() {
        return "esql_get_async_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncResultRequest get = new GetAsyncResultRequest(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            get.setWaitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout", get.getWaitForCompletionTimeout()));
        }
        if (request.hasParam("keep_alive")) {
            get.setKeepAlive(request.paramAsTime("keep_alive", get.getKeepAlive()));
        }
        return channel -> client.execute(EsqlAsyncGetResultAction.INSTANCE, get, new EsqlResponseListener(channel, request));
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, DROP_NULL_COLUMNS_OPTION);
    }
}
