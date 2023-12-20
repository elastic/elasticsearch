/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestGetFlamegraphAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_profiling/flamegraph"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetStackTracesRequest getStackTracesRequest = new GetStackTracesRequest();
        request.applyContentParser(getStackTracesRequest::parseXContent);
        // enforce server-side adjustment of sample counts for flamegraphs
        getStackTracesRequest.setAdjustSampleCount(true);

        return channel -> {
            RestActionListener<GetFlamegraphResponse> listener = new RestChunkedToXContentListener<>(channel);
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(GetFlamegraphAction.INSTANCE, getStackTracesRequest, listener);
        };
    }

    @Override
    public String getName() {
        return "get_flamegraph_action";
    }
}
