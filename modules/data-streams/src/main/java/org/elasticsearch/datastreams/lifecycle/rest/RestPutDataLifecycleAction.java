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
import org.elasticsearch.datastreams.lifecycle.action.PutDataLifecycleAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataLifecycleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_data_lifecycles_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}/_lifecycle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            PutDataLifecycleAction.Request putLifecycleRequest = PutDataLifecycleAction.Request.parseRequest(parser);
            putLifecycleRequest.indices(Strings.splitStringByCommaToArray(request.param("name")));
            putLifecycleRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putLifecycleRequest.masterNodeTimeout()));
            putLifecycleRequest.timeout(request.paramAsTime("timeout", putLifecycleRequest.timeout()));
            putLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(request, putLifecycleRequest.indicesOptions()));
            return channel -> client.execute(PutDataLifecycleAction.INSTANCE, putLifecycleRequest, new RestToXContentListener<>(channel));
        }
    }
}
