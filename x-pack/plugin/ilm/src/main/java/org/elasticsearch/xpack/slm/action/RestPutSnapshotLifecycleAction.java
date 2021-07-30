/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutSnapshotLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_slm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "slm_put_lifecycle";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String snapLifecycleName = request.param("name");
        try (XContentParser parser = request.contentParser()) {
            PutSnapshotLifecycleAction.Request req = PutSnapshotLifecycleAction.Request.parseRequest(snapLifecycleName, parser);
            req.timeout(request.paramAsTime("timeout", req.timeout()));
            req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));
            return channel -> client.execute(PutSnapshotLifecycleAction.INSTANCE, req, new RestToXContentListener<>(channel));
        }
    }
}
