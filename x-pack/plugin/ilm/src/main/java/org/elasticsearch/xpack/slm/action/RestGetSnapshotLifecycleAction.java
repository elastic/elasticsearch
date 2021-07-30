/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetSnapshotLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_slm/policy"),
            new Route(GET, "/_slm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "slm_get_lifecycle";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] lifecycleNames = Strings.splitStringByCommaToArray(request.param("name"));
        GetSnapshotLifecycleAction.Request req = new GetSnapshotLifecycleAction.Request(lifecycleNames);
        req.timeout(request.paramAsTime("timeout", req.timeout()));
        req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));

        return channel -> client.execute(GetSnapshotLifecycleAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
