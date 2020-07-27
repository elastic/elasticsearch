/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;


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
        DeleteAsyncResultRequest delete = new DeleteAsyncResultRequest(request.param("id"));
        return channel -> client.execute(DeleteAsyncResultAction.INSTANCE, delete, new RestToXContentListener<>(channel));
    }
}
