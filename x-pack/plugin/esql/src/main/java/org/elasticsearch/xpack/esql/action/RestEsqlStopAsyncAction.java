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
import org.elasticsearch.xpack.core.async.AsyncStopRequest;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.esql.action.EsqlQueryResponse.DROP_NULL_COLUMNS_OPTION;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

@ServerlessScope(Scope.PUBLIC)
public class RestEsqlStopAsyncAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_query/async/{id}/stop"));
    }

    @Override
    public String getName() {
        return "esql_async_stop";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AsyncStopRequest stopReq = new AsyncStopRequest(request.param("id"));
        return channel -> client.execute(EsqlAsyncStopAction.INSTANCE, stopReq, new EsqlResponseListener(channel, request));
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(URL_PARAM_DELIMITER, DROP_NULL_COLUMNS_OPTION);
    }
}
