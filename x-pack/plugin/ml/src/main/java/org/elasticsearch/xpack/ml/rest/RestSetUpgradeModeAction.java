/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSetUpgradeModeAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestSetUpgradeModeAction.class));

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(POST, MachineLearning.BASE_PATH + "set_upgrade_mode",
                POST, MachineLearning.PRE_V7_BASE_PATH + "set_upgrade_mode",
                deprecationLogger)
        );
    }

    @Override
    public String getName() {
        return "ml_set_upgrade_mode_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        SetUpgradeModeAction.Request request =
            new SetUpgradeModeAction.Request(restRequest.paramAsBoolean("enabled", false));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.execute(SetUpgradeModeAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
