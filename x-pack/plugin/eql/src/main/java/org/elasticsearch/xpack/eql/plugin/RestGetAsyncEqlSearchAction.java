/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.eql.action.GetAsyncEqlSearchAction;

import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.eql.plugin.RestSubmitAsyncEqlSearchAction.RESPONSE_PARAMS;

public class RestGetAsyncEqlSearchAction extends BaseRestHandler  {
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_eql/async_search/{id}")));
    }


    @Override
    public String getName() {
        return "async_eql_search_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncEqlSearchAction.Request get = new GetAsyncEqlSearchAction.Request(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            get.setWaitForCompletion(request.paramAsTime("wait_for_completion_timeout", get.getWaitForCompletion()));
        }
        if (request.hasParam("keep_alive")) {
            get.setKeepAlive(request.paramAsTime("keep_alive", get.getKeepAlive()));
        }
        return channel -> client.execute(GetAsyncEqlSearchAction.INSTANCE, get, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}
