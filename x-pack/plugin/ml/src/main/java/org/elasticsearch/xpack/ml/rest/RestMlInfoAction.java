/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestMlInfoAction extends BaseRestHandler {

    public RestMlInfoAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "info", this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.V7_BASE_PATH + "info", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_info_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        return channel -> client.execute(MlInfoAction.INSTANCE, new MlInfoAction.Request(), new RestToXContentListener<>(channel));
    }
}
