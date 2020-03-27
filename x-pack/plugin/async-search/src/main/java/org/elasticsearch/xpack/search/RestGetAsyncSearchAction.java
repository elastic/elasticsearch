/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.search.RestSubmitAsyncSearchAction.RESPONSE_PARAMS;

public class RestGetAsyncSearchAction extends BaseRestHandler  {
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_async_search/{id}")));
    }


    @Override
    public String getName() {
        return "async_search_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncSearchAction.Request get = new GetAsyncSearchAction.Request(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            get.setWaitForCompletion(request.paramAsTime("wait_for_completion_timeout", get.getWaitForCompletion()));
        }
        if (request.hasParam("keep_alive")) {
            get.setKeepAlive(request.paramAsTime("keep_alive", get.getKeepAlive()));
        }
        return channel -> client.execute(GetAsyncSearchAction.INSTANCE, get, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
