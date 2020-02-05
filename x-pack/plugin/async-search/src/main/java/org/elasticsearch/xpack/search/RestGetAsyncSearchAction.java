/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetAsyncSearchAction extends BaseRestHandler  {

    public RestGetAsyncSearchAction(RestController controller) {
        controller.registerHandler(GET, "/_async_search/{id}", this);
    }

    @Override
    public String getName() {
        return "async_search_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetAsyncSearchAction.Request get = new GetAsyncSearchAction.Request(request.param("id"));
        if (request.hasParam("wait_for_completion")) {
            get.setWaitForCompletion(request.paramAsTime("wait_for_completion", get.getWaitForCompletion()));
        }
        if (request.hasParam("keep_alive")) {
            get.setKeepAlive(request.paramAsTime("keep_alive", get.getKeepAlive()));
        }
        if (request.hasParam("last_version")) {
            get.setLastVersion(request.paramAsInt("last_version", get.getLastVersion()));
        }
        ActionRequestValidationException validationException = get.validate();
        if (validationException != null) {
            throw validationException;
        }
        return channel -> client.execute(GetAsyncSearchAction.INSTANCE, get, new RestStatusToXContentListener<>(channel));
    }
}
