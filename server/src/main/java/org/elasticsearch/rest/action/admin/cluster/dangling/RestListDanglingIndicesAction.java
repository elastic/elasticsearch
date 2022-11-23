/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster.dangling;

import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestListDanglingIndicesAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_dangling"));
    }

    @Override
    public String getName() {
        return "list_dangling_indices";
    }

    @Override
    public BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final ListDanglingIndicesRequest danglingIndicesRequest = new ListDanglingIndicesRequest();
        return channel -> client.admin()
            .cluster()
            .listDanglingIndices(danglingIndicesRequest, new RestActions.NodesResponseRestListener<>(channel));
    }
}
