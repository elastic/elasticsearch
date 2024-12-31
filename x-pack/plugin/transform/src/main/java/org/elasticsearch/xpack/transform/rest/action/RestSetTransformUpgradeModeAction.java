/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.transform.action.SetTransformUpgradeModeAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.xpack.core.transform.TransformField.REST_BASE_PATH_TRANSFORMS;

@ServerlessScope(Scope.INTERNAL)
public class RestSetTransformUpgradeModeAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, REST_BASE_PATH_TRANSFORMS + "set_upgrade_mode"));
    }

    @Override
    public String getName() {
        return "transform_set_upgrade_mode_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SetUpgradeModeActionRequest request = new SetUpgradeModeActionRequest(restRequest.paramAsBoolean("enabled", false));
        request.ackTimeout(getAckTimeout(restRequest));
        request.masterNodeTimeout(getMasterNodeTimeout(restRequest));
        return channel -> client.execute(SetTransformUpgradeModeAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
