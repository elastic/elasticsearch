/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;

import java.io.IOException;

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
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        int lastVersion = request.paramAsInt("last_version", -1);
        TimeValue waitForCompletion = request.paramAsTime("wait_for_completion", TimeValue.timeValueSeconds(1));
        TimeValue keepAlive = request.paramAsTime("keep_alive", TimeValue.MINUS_ONE);
        GetAsyncSearchAction.Request get = new GetAsyncSearchAction.Request(id, waitForCompletion, lastVersion);
        get.setKeepAlive(keepAlive);
        return channel -> client.execute(GetAsyncSearchAction.INSTANCE, get, new RestStatusToXContentListener<>(channel));
    }
}
