/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestSetUpgradeModeAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "set_upgrade_mode")
                .replaces(POST, PRE_V7_BASE_PATH + "set_upgrade_mode", RestApiVersion.V_7)
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_set_upgrade_mode_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SetUpgradeModeAction.Request request = new SetUpgradeModeAction.Request(restRequest.paramAsBoolean("enabled", false));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(SetUpgradeModeAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
