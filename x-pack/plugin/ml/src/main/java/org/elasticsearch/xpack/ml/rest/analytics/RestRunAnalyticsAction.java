/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.analytics;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.RunAnalyticsAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

public class RestRunAnalyticsAction extends BaseRestHandler {

    public RestRunAnalyticsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "analytics/{index}/_run", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_run_analytics_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        RunAnalyticsAction.Request request = new RunAnalyticsAction.Request(restRequest.param("index"));
        return channel -> {
            client.execute(RunAnalyticsAction.INSTANCE, request, new RestToXContentListener<>(channel));
        };
    }
}
