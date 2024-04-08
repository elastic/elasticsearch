/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.datastreams.lifecycle.action.DeleteDataStreamLifecycleAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteDataStreamLifecycleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_data_lifecycles_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_data_stream/{name}/_lifecycle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteDataStreamLifecycleAction.Request deleteDataLifecycleRequest = new DeleteDataStreamLifecycleAction.Request(
            Strings.splitStringByCommaToArray(request.param("name"))
        );
        deleteDataLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(request, deleteDataLifecycleRequest.indicesOptions()));
        return channel -> client.execute(
            DeleteDataStreamLifecycleAction.INSTANCE,
            deleteDataLifecycleRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
