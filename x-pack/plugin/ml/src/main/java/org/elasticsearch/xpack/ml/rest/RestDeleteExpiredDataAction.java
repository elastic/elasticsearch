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
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;

import java.io.IOException;

public class RestDeleteExpiredDataAction extends BaseRestHandler {

    public RestDeleteExpiredDataAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE, MachineLearning.BASE_PATH + "_delete_expired_data", this);
        controller.registerHandler(RestRequest.Method.DELETE, MachineLearning.V7_BASE_PATH + "_delete_expired_data", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_delete_expired_data_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteExpiredDataAction.Request request = new DeleteExpiredDataAction.Request();
        return channel -> client.execute(DeleteExpiredDataAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
