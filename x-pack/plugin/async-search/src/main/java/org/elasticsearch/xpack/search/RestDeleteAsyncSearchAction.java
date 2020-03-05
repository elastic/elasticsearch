/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.search.action.DeleteAsyncSearchAction;


import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAsyncSearchAction extends BaseRestHandler  {
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(DELETE, "/_async_search/{id}")));
    }

    @Override
    public String getName() {
        return "async_search_delete_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeleteAsyncSearchAction.Request delete = new DeleteAsyncSearchAction.Request(request.param("id"));
        return channel -> client.execute(DeleteAsyncSearchAction.INSTANCE, delete, new RestToXContentListener<>(channel));
    }
}
