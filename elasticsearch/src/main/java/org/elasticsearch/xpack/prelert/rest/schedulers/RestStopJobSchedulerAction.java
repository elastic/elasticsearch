/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.StopJobSchedulerAction;

import java.io.IOException;

public class RestStopJobSchedulerAction extends BaseRestHandler {

    private static final ParseField JOB_ID = new ParseField("jobId");

    private final StopJobSchedulerAction.TransportAction transportJobSchedulerAction;

    @Inject
    public RestStopJobSchedulerAction(Settings settings, RestController controller,
            StopJobSchedulerAction.TransportAction transportJobSchedulerAction) {
        super(settings);
        this.transportJobSchedulerAction = transportJobSchedulerAction;
        controller.registerHandler(RestRequest.Method.POST, PrelertPlugin.BASE_PATH + "schedulers/{jobId}/_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        StopJobSchedulerAction.Request jobSchedulerRequest = new StopJobSchedulerAction.Request(
                restRequest.param(JOB_ID.getPreferredName()));
        return channel -> transportJobSchedulerAction.execute(jobSchedulerRequest, new AcknowledgedRestListener<>(channel));
    }
}
