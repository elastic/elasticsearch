/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotRetentionAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestExecuteSnapshotRetentionAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_slm/_execute_retention"));
    }

    @Override
    public String getName() {
        return "slm_execute_retention";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        ExecuteSnapshotRetentionAction.Request req = new ExecuteSnapshotRetentionAction.Request();
        req.timeout(request.paramAsTime("timeout", req.timeout()));
        req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));
        return channel -> client.execute(ExecuteSnapshotRetentionAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
