/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.job;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.job.Job;

import java.io.IOException;

public class RestCloseJobAction extends BaseRestHandler {

    @Inject
    public RestCloseJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MlPlugin.BASE_PATH
                + "anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_close", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        CloseJobAction.Request request = new CloseJobAction.Request(restRequest.param(Job.ID.getPreferredName()));
        if (restRequest.hasParam("close_timeout")) {
            request.setCloseTimeout(TimeValue.parseTimeValue(restRequest.param("close_timeout"), "close_timeout"));
        }
        return channel -> client.execute(CloseJobAction.INSTANCE, request, new AcknowledgedRestListener<>(channel));
    }
}
