/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.schedulers;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.DeleteSchedulerAction;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestDeleteSchedulerAction extends BaseRestHandler {

    @Inject
    public RestDeleteSchedulerAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE, MlPlugin.BASE_PATH + "schedulers/{"
                + SchedulerConfig.ID.getPreferredName() + "}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String schedulerId = restRequest.param(SchedulerConfig.ID.getPreferredName());
        DeleteSchedulerAction.Request deleteSchedulerRequest = new DeleteSchedulerAction.Request(schedulerId);
        return channel -> client.execute(DeleteSchedulerAction.INSTANCE, deleteSchedulerRequest, new AcknowledgedRestListener<>(channel));
    }

}
