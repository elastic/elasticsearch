/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;

import java.io.IOException;
import java.util.List;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteRegionPolicyAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "inference_delete_region_policy_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "/_inference/_region_policy"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> client.execute(
            DeleteRegionPolicyAction.INSTANCE,
            new DeleteRegionPolicyAction.Request(),
            new RestToXContentListener<>(channel)
        );
    }
}
