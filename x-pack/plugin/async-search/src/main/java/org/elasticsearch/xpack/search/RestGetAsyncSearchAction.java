/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.search.RestSubmitAsyncSearchAction.RESPONSE_PARAMS;

@ServerlessScope(Scope.PUBLIC)
public class RestGetAsyncSearchAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_async_search/{id}"));
    }

    @Override
    public String getName() {
        return "async_search_get_action";
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
        return channel -> client.execute(GetAsyncSearchAction.INSTANCE, get, new RestRefCountedChunkedToXContentListener<>(channel) {
            @Override
            protected RestStatus getRestStatus(AsyncSearchResponse asyncSearchResponse) {
                return asyncSearchResponse.status();
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
